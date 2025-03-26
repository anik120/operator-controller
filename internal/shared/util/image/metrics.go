package image

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	containersImageResolveDigestCount = "containers_image_resolve_digest_count"
	containersImagePullImageCount     = "containers_image_pull_image_count"
	ownerIDLabel                      = "owner_id"
)

func init() {
	metrics.Registry.MustRegister(resolveDigestCount)
	metrics.Registry.MustRegister(pullImageCount)
}

var (
	resolveDigestCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: containersImageResolveDigestCount,
			Help: "The number of times the containers/image registry client has resolved a reference to a digest",
		},
		[]string{ownerIDLabel},
	)
	pullImageCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: containersImagePullImageCount,
			Help: "The number of times the containers/image registry client has copied an image to local disk",
		},
		[]string{ownerIDLabel},
	)
)

func incrementResolveDigestCount(ownerID string) {
	resolveDigestCount.With(prometheus.Labels{
		ownerIDLabel: ownerID,
	}).Inc()
}

func incrementPullImageCount(ownerID string) {
	pullImageCount.With(prometheus.Labels{
		ownerIDLabel: ownerID,
	}).Inc()
}
