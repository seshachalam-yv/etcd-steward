// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/transport/spdy"
)

var (
	kubeClient *kubernetes.Clientset
	restConfig *rest.Config

	etcdImage    string
	stewardImage string
)

func TestMain(m *testing.M) {
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("HOME") + "/.kube/config"
	}

	var err error
	restConfig, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build kubeconfig: %v\n", err)
		os.Exit(1)
	}

	kubeClient, err = kubernetes.NewForConfig(restConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create kubernetes client: %v\n", err)
		os.Exit(1)
	}

	etcdImage = os.Getenv("ETCD_IMAGE")
	if etcdImage == "" {
		etcdImage = "gcr.io/etcd-development/etcd:v3.5.12"
	}

	stewardImage = os.Getenv("STEWARD_IMAGE")
	if stewardImage == "" {
		stewardImage = "localhost/etcd-steward:latest"
	}

	os.Exit(m.Run())
}

// setupNamespace creates a per-test namespace derived from t.Name().
// Slashes and underscores are replaced with dashes and the name is truncated to 63 characters.
func setupNamespace(t *testing.T) string {
	t.Helper()

	name := strings.ToLower(t.Name())
	name = strings.ReplaceAll(name, "/", "-")
	name = strings.ReplaceAll(name, "_", "-")
	if len(name) > 63 {
		name = name[:63]
	}
	// Remove trailing dashes
	name = strings.TrimRight(name, "-")

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}

	ctx := context.Background()
	_, err := kubeClient.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create namespace %s: %v", name, err)
	}

	t.Cleanup(func() {
		_ = kubeClient.CoreV1().Namespaces().Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	return name
}

// createEtcdStewardPod creates a pod with an etcd container and an etcd-steward sidecar container.
func createEtcdStewardPod(t *testing.T, namespace, podName string, stewardArgs []string) *corev1.Pod {
	t.Helper()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "etcd",
					Image: etcdImage,
					Command: []string{
						"etcd",
						"--listen-client-urls=http://0.0.0.0:2379",
						"--advertise-client-urls=http://127.0.0.1:2379",
						"--listen-peer-urls=http://0.0.0.0:2380",
						"--initial-advertise-peer-urls=http://127.0.0.1:2380",
						"--initial-cluster=default=http://127.0.0.1:2380",
						"--data-dir=/var/etcd/data/new.etcd",
					},
					Ports: []corev1.ContainerPort{
						{Name: "client", ContainerPort: 2379, Protocol: corev1.ProtocolTCP},
						{Name: "peer", ContainerPort: 2380, Protocol: corev1.ProtocolTCP},
					},
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							TCPSocket: &corev1.TCPSocketAction{
								Port: intstr.FromInt32(2379),
							},
						},
						InitialDelaySeconds: 5,
						PeriodSeconds:       5,
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "etcd-data", MountPath: "/var/etcd/data"},
					},
				},
				{
					Name:  "steward",
					Image: stewardImage,
					Args:  stewardArgs,
					Ports: []corev1.ContainerPort{
						{Name: "steward", ContainerPort: 8080, Protocol: corev1.ProtocolTCP},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "etcd-data", MountPath: "/var/etcd/data"},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "etcd-data",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
		},
	}

	ctx := context.Background()
	created, err := kubeClient.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create pod %s/%s: %v", namespace, podName, err)
	}

	t.Cleanup(func() {
		_ = kubeClient.CoreV1().Pods(namespace).Delete(context.Background(), podName, metav1.DeleteOptions{})
	})

	return created
}

// createEtcdStewardPodWithVolumes creates a pod with an etcd and steward container using emptyDir
// for shared /var/etcd/data. This is identical to createEtcdStewardPod but provided as an
// explicit helper for tests that emphasise the shared volume aspect.
func createEtcdStewardPodWithVolumes(t *testing.T, namespace, podName string, stewardArgs []string) *corev1.Pod {
	t.Helper()
	return createEtcdStewardPod(t, namespace, podName, stewardArgs)
}

// waitForPodReady waits until all containers in the pod are in Running state.
func waitForPodReady(t *testing.T, namespace, podName string, timeout time.Duration) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for pod %s/%s to become ready", namespace, podName)
		case <-ticker.C:
			pod, err := kubeClient.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
			if err != nil {
				continue
			}
			if pod.Status.Phase != corev1.PodRunning {
				continue
			}
			allRunning := true
			for _, cs := range pod.Status.ContainerStatuses {
				if cs.State.Running == nil {
					allRunning = false
					break
				}
			}
			if allRunning && len(pod.Status.ContainerStatuses) > 0 {
				return
			}
		}
	}
}

// waitForStewardHealthy waits until the steward container is in Running state.
// Since the steward container uses a distroless image, we cannot exec into it;
// instead we check the container status via the Kubernetes API.
func waitForStewardHealthy(t *testing.T, namespace, podName string, timeout time.Duration) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for steward container in pod %s/%s to be healthy", namespace, podName)
		case <-ticker.C:
			pod, err := kubeClient.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
			if err != nil {
				continue
			}
			for _, cs := range pod.Status.ContainerStatuses {
				if cs.Name == "steward" && cs.State.Running != nil {
					return
				}
			}
		}
	}
}

// execInPod executes a command in the specified container of a pod and returns stdout and stderr.
func execInPod(t *testing.T, namespace, podName, containerName string, command []string) (string, string) {
	t.Helper()

	req := kubeClient.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(restConfig, http.MethodPost, req.URL())
	if err != nil {
		t.Fatalf("failed to create SPDY executor: %v", err)
	}

	var stdout, stderr bytes.Buffer
	err = executor.StreamWithContext(context.Background(), remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		t.Fatalf("exec failed in pod %s/%s container %s: %v\nstdout: %s\nstderr: %s",
			namespace, podName, containerName, err, stdout.String(), stderr.String())
	}

	return stdout.String(), stderr.String()
}

// portForwardPod opens a port-forward to the given pod and makes an HTTP request to the specified path.
// It returns the response body as a string.
func portForwardPod(t *testing.T, namespace, podName string, podPort int, httpPath string) string {
	t.Helper()

	transport, upgrader, err := spdy.RoundTripperFor(restConfig)
	if err != nil {
		t.Fatalf("failed to create SPDY round tripper: %v", err)
	}

	req := kubeClient.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("portforward")

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, req.URL())

	stopChan := make(chan struct{}, 1)
	readyChan := make(chan struct{})

	ports := []string{fmt.Sprintf("0:%d", podPort)}
	fw, err := portforward.New(dialer, ports, stopChan, readyChan, io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("failed to create port forwarder: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- fw.ForwardPorts()
	}()

	select {
	case <-readyChan:
	case err := <-errCh:
		t.Fatalf("port-forward failed: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for port-forward to be ready")
	}

	defer close(stopChan)

	forwardedPorts, err := fw.GetPorts()
	if err != nil || len(forwardedPorts) == 0 {
		t.Fatalf("failed to get forwarded ports: %v", err)
	}

	localPort := forwardedPorts[0].Local
	url := fmt.Sprintf("http://127.0.0.1:%d%s", localPort, httpPath)

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("HTTP GET %s failed: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body from %s: %v", url, err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTP GET %s returned status %d: %s", url, resp.StatusCode, string(body))
	}

	return string(body)
}

// getPodLogs returns the logs from a specific container in a pod.
func getPodLogs(t *testing.T, namespace, podName, containerName string) string {
	t.Helper()

	req := kubeClient.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: containerName,
	})

	stream, err := req.Stream(context.Background())
	if err != nil {
		t.Fatalf("failed to get logs for pod %s/%s container %s: %v", namespace, podName, containerName, err)
	}
	defer func() { _ = stream.Close() }()

	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, stream); err != nil {
		t.Fatalf("failed to read logs: %v", err)
	}

	return buf.String()
}

// waitForLogEntry polls pod logs until a line containing the expected substring appears.
func waitForLogEntry(t *testing.T, namespace, podName, containerName, expected string, timeout time.Duration) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logs := getPodLogs(t, namespace, podName, containerName)
			t.Fatalf("timed out waiting for log entry %q in pod %s/%s container %s.\nLast logs:\n%s",
				expected, namespace, podName, containerName, logs)
		case <-ticker.C:
			logs := getPodLogs(t, namespace, podName, containerName)
			if strings.Contains(logs, expected) {
				return
			}
		}
	}
}

// defaultStewardArgs returns the standard steward container args for a single-node e2e pod.
func defaultStewardArgs(podName, namespace string) []string {
	return []string{
		fmt.Sprintf("--pod-name=%s", podName),
		fmt.Sprintf("--pod-namespace=%s", namespace),
		"--server-port=8080",
		"--etcd-endpoints=http://127.0.0.1:2379",
		"--data-dir=/var/etcd/data/new.etcd",
		"--enable-member-lease-renewal=false",
		"--enable-snapshot-lease-renewal=false",
		"--enable-snapshotter=false",
		"--enable-gc=false",
		"--enable-defrag=false",
		"--enable-alarm-handler=false",
	}
}
