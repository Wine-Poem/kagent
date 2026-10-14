package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

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
	warmKeyLabel       = "ate.dev/warm-key"
	actorNameAnno      = "ate.dev/actor-name"
	actorAtespaceAnno  = "ate.dev/actor-atespace"
	actorUIDAnno       = "ate.dev/actor-uid"
	trustBundleMap     = "ate-trust-bundle"
	trustBundleKey     = "trust-bundle.pem"
	trustBundleSrcPath = "/etc/ssl/certs/ca-certificates.crt"
	helperBinary       = "/fargate-shim"
	wrapperDir         = "/ate"
	wrapperPath        = wrapperDir + "/fargate-shim"
	sidecarPort        = 9090
	wrapperPort        = 9091
)

type podSettings struct {
	CPU            string
	Memory         string
	ServiceAccount string
	RunAsRoot      bool
	// HelperImage is this program's own image. It provides the restore init container,
	// the snapshot sidecar and the wrapper that warm pods start the runtime with.
	HelperImage string
	Persistence bool
}

type startRequest struct {
	Actor    *ateapipb.Actor
	Template *ateapipb.ActorTemplate
	Snapshot *snapshotRef
}

type podInfo struct {
	ref     ref
	warmKey string
	created time.Time
}

type k8sPods struct {
	client   kubernetes.Interface
	settings podSettings
	warm     *warmPool
}

func (p *k8sPods) Start(ctx context.Context, req startRequest) (string, error) {
	actor := req.Actor
	namespace := actor.GetMetadata().GetAtespace()
	if err := p.ensureTrustBundle(ctx, namespace); err != nil {
		return "", err
	}
	name := actor.GetMetadata().GetName()
	existing, err := p.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	switch {
	case err == nil && existing.Annotations[actorUIDAnno] == actor.GetMetadata().GetUid() && existing.DeletionTimestamp == nil:
		return name, nil
	case err == nil:
		if err := p.Delete(ctx, namespace, name); err != nil {
			return "", err
		}
	case !apierrors.IsNotFound(err):
		return "", err
	}
	if p.warm != nil {
		if claimed, ok := p.warm.claim(ctx, p, req); ok {
			return claimed, nil
		}
	}
	pod, err := buildPod(podOptions{Name: name, Actor: actor, Template: req.Template, Settings: p.settings, Snapshot: req.Snapshot})
	if err != nil {
		return "", err
	}
	_, err = p.client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return "", err
	}
	return name, nil
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

func (p *k8sPods) List(ctx context.Context) ([]podInfo, error) {
	list, err := p.client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{LabelSelector: managedByLabel + "=" + managedByValue})
	if err != nil {
		return nil, err
	}
	var out []podInfo
	for _, pod := range list.Items {
		out = append(out, podInfo{ref: ref{pod.Namespace, pod.Name}, warmKey: pod.Labels[warmKeyLabel], created: pod.CreationTimestamp.Time})
	}
	return out, nil
}

func (p *k8sPods) Hint(template *ateapipb.ActorTemplate) {
	if p.warm != nil {
		p.warm.touch(template, p.settings)
	}
}

func (p *k8sPods) Maintain(ctx context.Context) {
	if p.warm != nil {
		p.warm.maintain(ctx, p)
	}
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

type podOptions struct {
	Name     string
	Actor    *ateapipb.Actor // nil for a warm pod that no actor has claimed yet
	Template *ateapipb.ActorTemplate
	Settings podSettings
	Snapshot *snapshotRef
	// WarmKey marks a pod that no actor has claimed yet. Its main container runs the
	// wrapper, which waits for an activation and then starts the runtime.
	WarmKey string
}

func (o podOptions) warm() bool { return o.WarmKey != "" }

// durableVolume returns the volume that holds the runtime's private /data and its mount path.
func durableVolume(template *ateapipb.ActorTemplate) (name, mount string) {
	c := template.GetContainers()[0]
	for _, v := range template.GetVolumes() {
		if v.GetDurableDir() == nil {
			continue
		}
		for _, m := range c.GetVolumeMounts() {
			if m.GetName() == v.GetName() {
				return v.GetName(), m.GetMountPath()
			}
		}
	}
	return "", ""
}

// identityVolume returns the volume that carries the actor identity files, their paths and the mount path.
func identityVolume(template *ateapipb.ActorTemplate) (name, mount string, fields map[string]ateapipb.ActorMetadataField) {
	c := template.GetContainers()[0]
	for _, v := range template.GetVolumes() {
		items := map[string]ateapipb.ActorMetadataField{}
		for _, ds := range v.GetSystemInfo().GetDataSources() {
			for _, item := range ds.GetActorMetadata().GetItems() {
				items[item.GetPath()] = item.GetField()
			}
		}
		if len(items) == 0 {
			continue
		}
		for _, m := range c.GetVolumeMounts() {
			if m.GetName() == v.GetName() {
				return v.GetName(), m.GetMountPath(), items
			}
		}
	}
	return "", "", nil
}

// buildPod renders one Actor as one pod. The compute-type label selects the
// Fargate profile, so every actor gets its own Firecracker microVM.
func buildPod(o podOptions) (*corev1.Pod, error) {
	template, settings := o.Template, o.Settings
	if len(template.GetContainers()) != 1 {
		return nil, fmt.Errorf("template must have exactly one container")
	}
	c := template.GetContainers()[0]
	var env []corev1.EnvVar
	if !o.warm() {
		for _, e := range c.GetEnv() {
			env = append(env, corev1.EnvVar{Name: e.GetName(), Value: e.GetValue()})
		}
	}
	identityName, _, _ := identityVolume(template)
	var volumes []corev1.Volume
	for _, v := range template.GetVolumes() {
		switch {
		case v.GetDurableDir() != nil:
			volumes = append(volumes, corev1.Volume{Name: v.GetName(), VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
		case v.GetSystemInfo() != nil && o.warm() && v.GetName() == identityName:
			// The wrapper writes the identity files when it activates the pod.
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
	container := corev1.Container{
		Name: c.GetName(), Image: c.GetImage(), Command: c.GetCommand(), Args: c.GetArgs(),
		Env: env, VolumeMounts: mounts, Resources: requests(cpu, memory),
		Ports: []corev1.ContainerPort{{Name: "a2a", ContainerPort: 80}, {Name: "ready", ContainerPort: 8081}},
	}
	var initContainers []corev1.Container
	root := int64(0)
	helperSecurity := &corev1.SecurityContext{RunAsUser: &root}
	if o.warm() {
		volumes = append(volumes, corev1.Volume{Name: "ate-bin", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "ate-bin", MountPath: wrapperDir})
		container.Command, container.Args = []string{wrapperPath, "wrapper"}, nil
		initContainers = append(initContainers, corev1.Container{
			Name: "install-wrapper", Image: settings.HelperImage, Command: []string{helperBinary, "install", "--dest=" + wrapperPath},
			VolumeMounts: []corev1.VolumeMount{{Name: "ate-bin", MountPath: wrapperDir}}, SecurityContext: helperSecurity,
			Resources: requests(resource.MustParse("50m"), resource.MustParse("64Mi")),
		})
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
	containers := []corev1.Container{container}
	if dataName, dataMount := durableVolume(template); settings.Persistence && settings.HelperImage != "" && dataName != "" {
		dataMounts := []corev1.VolumeMount{{Name: dataName, MountPath: dataMount}}
		containers = append(containers, corev1.Container{
			Name: "snapshotter", Image: settings.HelperImage, SecurityContext: helperSecurity, VolumeMounts: dataMounts,
			Command:        []string{helperBinary, "sidecar", "--src=" + dataMount, fmt.Sprintf("--listen=:%d", sidecarPort)},
			Resources:      requests(resource.MustParse("100m"), resource.MustParse("128Mi")),
			ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt32(sidecarPort)}}, PeriodSeconds: 5},
		})
		if !o.warm() && o.Snapshot != nil {
			initContainers = append(initContainers, corev1.Container{
				Name: "restore-data", Image: settings.HelperImage, SecurityContext: helperSecurity, VolumeMounts: dataMounts,
				Command:   []string{helperBinary, "restore", "--url=" + o.Snapshot.URL, "--token=" + o.Snapshot.Token, "--dest=" + dataMount},
				Resources: requests(resource.MustParse("50m"), resource.MustParse("64Mi")),
			})
		}
	}
	no := false
	labels := map[string]string{computeTypeLabel: "fargate", managedByLabel: managedByValue}
	annotations := map[string]string{}
	if o.warm() {
		labels[warmKeyLabel] = o.WarmKey
	} else {
		annotations[actorNameAnno] = o.Actor.GetMetadata().GetName()
		annotations[actorAtespaceAnno] = o.Actor.GetMetadata().GetAtespace()
		annotations[actorUIDAnno] = o.Actor.GetMetadata().GetUid()
	}
	namespace := o.Actor.GetMetadata().GetAtespace()
	if o.warm() {
		namespace = template.GetMetadata().GetAtespace()
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: o.Name, Namespace: namespace, Labels: labels, Annotations: annotations},
		Spec: corev1.PodSpec{
			Containers: containers, InitContainers: initContainers, Volumes: volumes,
			RestartPolicy:                corev1.RestartPolicyAlways,
			ServiceAccountName:           settings.ServiceAccount,
			AutomountServiceAccountToken: &no,
			EnableServiceLinks:           &no,
		},
	}, nil
}

func requests(cpu, memory resource.Quantity) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: cpu, corev1.ResourceMemory: memory},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: cpu, corev1.ResourceMemory: memory},
	}
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
