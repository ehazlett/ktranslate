package dynatrace

import (
	"testing"

	"github.com/kentik/ktranslate/pkg/kt"
	"github.com/stretchr/testify/assert"
)

func TestSerializeDynatrace(t *testing.T) {
	serBuf := make([]byte, 0)
	assert := assert.New(t)

	f, err := NewFormat(nil)
	assert.NoError(err)

	res, err := f.To(kt.InputTesting, serBuf)
	assert.NoError(err)
	assert.NotNil(res)
}
