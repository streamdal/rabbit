package rabbit

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestRabbitSuite(t *testing.T) {

	RegisterFailHandler(Fail)
	RunSpecs(t, "Rabbit Suite")
}
