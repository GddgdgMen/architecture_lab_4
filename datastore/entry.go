package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
)

type entry struct {
	key, value, dataType string
}

func getLength(key string, value string, dataType string) int64 {
	return int64(len(key) + len(value) + len(dataType) + 12)
}

func (e *entry) GetLength() int64 {
	return getLength(e.key, e.value, e.dataType)
}

func (e *entry) Encode() []byte {
	kl := len(e.key)
	vl := len(e.value)
	size := kl + vl + 12
	res := make([]byte, size)
	binary.LittleEndian.PutUint32(res, uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], e.key)
	binary.LittleEndian.PutUint32(res[kl+8:], uint32(vl))
	copy(res[kl+12:], e.value)
	copy(res[kl+12+vl:], e.dataType)
	return res
}

func (e *entry) Decode(input []byte) {
	kl := binary.LittleEndian.Uint32(input[4:])
	keyBuf := make([]byte, kl)
	copy(keyBuf, input[8:kl+8])
	e.key = string(keyBuf)

	vl := binary.LittleEndian.Uint32(input[kl+8:])
	valBuf := make([]byte, vl)
	copy(valBuf, input[kl+12:kl+12+vl])
	e.value = string(valBuf)

	dl := binary.LittleEndian.Uint32(input[kl+12+vl:])
	dataType := make([]byte, dl)
	copy(dataType, input[kl+12+vl+4:kl+12+vl+4+dl])
	e.dataType = string(dataType)
}

func readValue(in *bufio.Reader) (string, error) {
	header, err := in.Peek(8)
	if err != nil {
		return "", err
	}
	keySize := int(binary.LittleEndian.Uint32(header[4:]))
	_, err = in.Discard(keySize + 8)
	if err != nil {
		return "", err
	}

	dataTypeBuf := make([]byte, 4)
	_, err = in.Read(dataTypeBuf)
	if err != nil {
		return "", err
	}
	dataType := string(dataTypeBuf)
	if dataType != string(dataTypeBuf) {
		return "", fmt.Errorf("unexpected data type: %s", dataType)
	}

	header, err = in.Peek(4)
	if err != nil {
		return "", err
	}
	valSize := int(binary.LittleEndian.Uint32(header))
	_, err = in.Discard(4)
	if err != nil {
		return "", err
	}

	data := make([]byte, valSize)
	n, err := in.Read(data)
	if err != nil {
		return "", err
	}
	if n != valSize {
		return "", fmt.Errorf("can't read value bytes (read %d, expected %d)", n, valSize)
	}

	return string(data), nil
}
