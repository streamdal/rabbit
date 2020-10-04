package rabbit

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

func TestRabbitSuite(t *testing.T) {
	// reduce the noise when testing
	logrus.SetLevel(logrus.DebugLevel)

	RegisterFailHandler(Fail)
	RunSpecs(t, "Rabbit Suite")
}
