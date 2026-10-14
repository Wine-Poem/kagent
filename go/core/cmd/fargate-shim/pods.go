package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

const (
	managedByLabel     = "app.kubernetes.io/managed-by"
	managedByValue     = "ate-fargate-shim"
	computeTypeLabel   = "compute-type"
	actorNameAnno      = "ate.dev/actor-name"
	actorAtespaceAnno  = "ate.dev/actor-atespace"
	actorUIDAnno       = "ate.dev/actor-uid"
	trustBundleMap     = "ate-trust-bundle"
	trustBundleKey     = "trust-bundle.pem"
	trustBundleSrcPath = "/etc/ssl/certs/ca-certificates.crt"
)

type podSettings struct {
	CPU            string
	Memory         string
	ServiceAccount string
	RunAsRoot      bool
}

type k8sPods struct {
	client   kubernetes.Interface
	settings podSettings
}

func (p *k8sPods) Ensure(ctx context.Context, actor *ateapipb.Actor, template *ateapipb.ActorTemplate) error {
	namespace := actor.GetMetadata().GetAtespace()
	if err := p.ensureTrustBundle(ctx, namespace); err != nil {
		return err
	}
	pod, err := buildPod(actor, template, p.settings)
	if err != nil {
		return err
	}
	existing, err := p.client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err == nil {
		if existing.Annotations[actorUIDAnno] == actor.GetMetadata().GetUid() && existing.DeletionTimestamp == nil {
			return nil
		}
		if err := p.Delete(ctx, namespace, pod.Name); err != nil {
			return err
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	_, err = p.client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (p *k8sPods) State(ctx context.Context, atespace, name string) (podState, error) {
	pod, err := p.client.CoreV1().Pods(atespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return podState{}, nil
	}
	if err != nil {
		return podState{}, err
	}
	state := podState{Exists: pod.DeletionTimestamp == nil, IP: pod.Status.PodIP, Node: pod.Spec.NodeName}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue && pod.DeletionTimestamp == nil {
			state.Ready = true
		}
	}
	return state, nil
}

func (p *k8sPods) Delete(ctx context.Context, atespace, name string) error {
	grace := int64(0)
	err := p.client.CoreV1().Pods(atespace).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &grace})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (p *k8sPods) ListManaged(ctx context.Context) ([]ref, error) {
	list, err := p.client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{LabelSelector: managedByLabel + "=" + managedByValue})
	if err != nil {
		return nil, err
	}
	var out []ref
	for _, pod := range list.Items {
		out = append(out, ref{pod.Namespace, pod.Name})
	}
	return out, nil
}

// ensureTrustBundle publishes the system CA bundle where the runtime expects the
// egress trust bundle. Without an egress proxy the bundle only needs the real roots.
func (p *k8sPods) ensureTrustBundle(ctx context.Context, namespace string) error {
	bundle, err := os.ReadFile(trustBundleSrcPath)
	if err != nil {
		return fmt.Errorf("read CA bundle: %w", err)
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: trustBundleMap, Namespace: namespace, Labels: map[string]string{managedByLabel: managedByValue + "-config"}},
		Data:       map[string]string{trustBundleKey: string(bundle)},
	}
	_, err = p.client.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func actorMetadataAnnotation(field ateapipb.ActorMetadataField) (string, error) {
	switch field {
	case ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME:
		return actorNameAnno, nil
	case ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE:
		return actorAtespaceAnno, nil
	case ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_UID:
		return actorUIDAnno, nil
	}
	return "", fmt.Errorf("unsupported actor metadata field %s", field)
}

// buildPod renders one Actor as one pod. The compute-type label selects the
// Fargate profile, so every actor gets its own Firecracker microVM.
func buildPod(actor *ateapipb.Actor, template *ateapipb.ActorTemplate, settings podSettings) (*corev1.Pod, error) {
	if len(template.GetContainers()) != 1 {
		return nil, fmt.Errorf("template must have exactly one container")
	}
	c := template.GetContainers()[0]
	var env []corev1.EnvVar
	for _, e := range c.GetEnv() {
		env = append(env, corev1.EnvVar{Name: e.GetName(), Value: e.GetValue()})
	}
	var volumes []corev1.Volume
	for _, v := range template.GetVolumes() {
		switch {
		case v.GetDurableDir() != nil:
			volumes = append(volumes, corev1.Volume{Name: v.GetName(), VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
		case v.GetSystemInfo() != nil:
			source, err := systemInfoVolume(v.GetSystemInfo())
			if err != nil {
				return nil, fmt.Errorf("volume %s: %w", v.GetName(), err)
			}
			volumes = append(volumes, corev1.Volume{Name: v.GetName(), VolumeSource: source})
		default:
			return nil, fmt.Errorf("volume %s: unsupported volume type", v.GetName())
		}
	}
	var mounts []corev1.VolumeMount
	for _, m := range c.GetVolumeMounts() {
		mounts = append(mounts, corev1.VolumeMount{Name: m.GetName(), MountPath: m.GetMountPath()})
	}
	cpu, memory := resource.MustParse(settings.CPU), resource.MustParse(settings.Memory)
	resources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: cpu, corev1.ResourceMemory: memory},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: cpu, corev1.ResourceMemory: memory},
	}
	container := corev1.Container{
		Name: c.GetName(), Image: c.GetImage(), Command: c.GetCommand(), Args: c.GetArgs(),
		Env: env, VolumeMounts: mounts, Resources: resources,
		Ports: []corev1.ContainerPort{{Name: "a2a", ContainerPort: 80}, {Name: "ready", ContainerPort: 8081}},
	}
	if probe := c.GetWakeupProbe().GetHttpGet(); probe != nil {
		container.ReadinessProbe = &corev1.Probe{
			ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: probe.GetPath(), Port: intstr.FromInt32(probe.GetPort())}},
			PeriodSeconds:    2,
			TimeoutSeconds:   3,
			FailureThreshold: 90,
		}
	}
	if settings.RunAsRoot {
		// The runtime image writes /config and binds port 80; Substrate runs it as root inside its sandbox.
		root := int64(0)
		container.SecurityContext = &corev1.SecurityContext{RunAsUser: &root}
	}
	if caps := c.GetSecurityContext().GetCapabilities(); caps != nil {
		sc := container.SecurityContext
		if sc == nil {
			sc = &corev1.SecurityContext{}
		}
		sc.Capabilities = &corev1.Capabilities{}
		for _, add := range caps.GetAdd() {
			sc.Capabilities.Add = append(sc.Capabilities.Add, corev1.Capability(add))
		}
		for _, drop := range caps.GetDrop() {
			sc.Capabilities.Drop = append(sc.Capabilities.Drop, corev1.Capability(drop))
		}
		container.SecurityContext = sc
	}
	no := false
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: actor.GetMetadata().GetName(), Namespace: actor.GetMetadata().GetAtespace(),
			Labels: map[string]string{computeTypeLabel: "fargate", managedByLabel: managedByValue},
			Annotations: map[string]string{
				actorNameAnno:     actor.GetMetadata().GetName(),
				actorAtespaceAnno: actor.GetMetadata().GetAtespace(),
				actorUIDAnno:      actor.GetMetadata().GetUid(),
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{container}, Volumes: volumes,
			RestartPolicy:                corev1.RestartPolicyAlways,
			ServiceAccountName:           settings.ServiceAccount,
			AutomountServiceAccountToken: &no,
			EnableServiceLinks:           &no,
		},
	}, nil
}

func systemInfoVolume(info *ateapipb.SystemInfoVolumeSource) (corev1.VolumeSource, error) {
	var items []corev1.DownwardAPIVolumeFile
	var trustPath string
	for _, ds := range info.GetDataSources() {
		for _, item := range ds.GetActorMetadata().GetItems() {
			anno, err := actorMetadataAnnotation(item.GetField())
			if err != nil {
				return corev1.VolumeSource{}, err
			}
			items = append(items, corev1.DownwardAPIVolumeFile{Path: item.GetPath(), FieldRef: &corev1.ObjectFieldSelector{FieldPath: fmt.Sprintf("metadata.annotations['%s']", anno)}})
		}
		if ds.GetTrustBundle() != nil {
			trustPath = ds.GetTrustBundle().GetPath()
		}
	}
	switch {
	case len(items) > 0 && trustPath == "":
		sort.Slice(items, func(i, j int) bool { return items[i].Path < items[j].Path })
		return corev1.VolumeSource{DownwardAPI: &corev1.DownwardAPIVolumeSource{Items: items}}, nil
	case trustPath != "" && len(items) == 0:
		return corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: trustBundleMap},
			Items:                []corev1.KeyToPath{{Key: trustBundleKey, Path: strings.TrimPrefix(trustPath, "/")}},
		}}, nil
	}
	return corev1.VolumeSource{}, fmt.Errorf("system info volume must hold either actor metadata or a trust bundle")
}
