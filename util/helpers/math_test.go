package helpers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetBit(t *testing.T) {
	require.True(t, GetBit(0b00000001, 0))
	require.True(t, GetBit(0b00000010, 1))
	require.True(t, GetBit(0b00000100, 2))
	require.True(t, GetBit(0b00001000, 3))
	require.True(t, GetBit(0b00010000, 4))
	require.True(t, GetBit(0b00100000, 5))
	require.True(t, GetBit(0b01000000, 6))
	require.True(t, GetBit(0b10000000, 7))
	
	require.False(t, GetBit(0b00000010, 0))
	require.False(t, GetBit(0b00000100, 1))
	require.False(t, GetBit(0b00001000, 2))
	require.False(t, GetBit(0b00010000, 3))
	require.False(t, GetBit(0b00100000, 4))
	require.False(t, GetBit(0b01000000, 5))
	require.False(t, GetBit(0b10000000, 6))
	require.False(t, GetBit(0b00000001, 7))
}

func TestSetBit(t *testing.T) {
	b := new(uint8)
	*b = 0

	SetBit(b, 0, true)
	require.Equal(t, uint8(0b00000001), *b)

	SetBit(b, 0, false)
	require.Equal(t, uint8(0b00000000), *b)
	
	SetBit(b, 4, true)
	require.Equal(t, uint8(0b00010000), *b)
	
	SetBit(b, 6, true)
	require.Equal(t, uint8(0b01010000), *b)
	
	SetBit(b, 1, true)
	require.Equal(t, uint8(0b01010010), *b)
	
	SetBit(b, 4, false)
	require.Equal(t, uint8(0b01000010), *b)
}
