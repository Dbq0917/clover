package bbolt

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ostafen/clover/v2/store"
)

func collect(t *testing.T, cursor store.Cursor, seek []byte) []string {
	t.Helper()
	require.NoError(t, cursor.Seek(seek))

	var keys []string
	for ; cursor.Valid(); cursor.Next() {
		item, err := cursor.Item()
		require.NoError(t, err)
		keys = append(keys, string(item.Key))
	}
	return keys
}

func TestCursorSeekSemantics(t *testing.T) {
	keys := [][]byte{
		[]byte("c:coll;i:f;t:1;v:a00000000-0000-0000-0000-000000000001"),
		[]byte("c:coll;i:f;t:1;v:a00000000-0000-0000-0000-000000000002"),
		[]byte("c:coll;i:f;t:1;v:b00000000-0000-0000-0000-000000000003"),
		[]byte("c:coll;i:f;t:1;v:c00000000-0000-0000-0000-000000000004"),
		[]byte("c:other;"),
	}

	dir, err := os.MkdirTemp("", "bolt-cursor-open")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := Open(dir)
	require.NoError(t, err)
	defer s.Close()

	tx, err := s.Begin(true)
	require.NoError(t, err)
	for _, key := range keys {
		require.NoError(t, tx.Set(key, nil))
	}
	require.NoError(t, tx.Commit())

	rtx, err := s.Begin(false)
	require.NoError(t, err)
	defer rtx.Rollback()

	t.Run("forward exact match", func(t *testing.T) {
		cursor, err := rtx.Cursor(true)
		require.NoError(t, err)
		defer cursor.Close()

		got := collect(t, cursor, keys[0])
		require.Equal(t, []string{
			string(keys[0]), string(keys[1]), string(keys[2]), string(keys[3]), string(keys[4]),
		}, got)
	})

	t.Run("forward overshoot lands on next key", func(t *testing.T) {
		cursor, err := rtx.Cursor(true)
		require.NoError(t, err)
		defer cursor.Close()

		got := collect(t, cursor, []byte("c:coll;i:f;t:1;v:aa"))
		require.Equal(t, []string{string(keys[2]), string(keys[3]), string(keys[4])}, got)
	})

	t.Run("reverse exact match", func(t *testing.T) {
		cursor, err := rtx.Cursor(false)
		require.NoError(t, err)
		defer cursor.Close()

		got := collect(t, cursor, keys[3])
		require.Equal(t, []string{string(keys[3]), string(keys[2]), string(keys[1]), string(keys[0])}, got)
	})

	t.Run("reverse overshoot steps back", func(t *testing.T) {
		cursor, err := rtx.Cursor(false)
		require.NoError(t, err)
		defer cursor.Close()

		got := collect(t, cursor, []byte("c:coll;i:f;t:1;v:ba"))
		require.Equal(t, []string{string(keys[2]), string(keys[1]), string(keys[0])}, got)
	})

	t.Run("reverse seek beyond last key lands on last key", func(t *testing.T) {
		cursor, err := rtx.Cursor(false)
		require.NoError(t, err)
		defer cursor.Close()

		got := collect(t, cursor, []byte("z"))
		require.Equal(t, []string{string(keys[4]), string(keys[3]), string(keys[2]), string(keys[1]), string(keys[0])}, got)
	})

	t.Run("reverse prefix successor iterates whole group", func(t *testing.T) {
		cursor, err := rtx.Cursor(false)
		require.NoError(t, err)
		defer cursor.Close()

		// Index range scans seek to the value prefix plus 0xff so that the
		// cursor starts on the last key of the end-value group.
		got := collect(t, cursor, []byte("c:coll;i:f;t:1;v:b\xff"))
		require.Equal(t, []string{string(keys[2]), string(keys[1]), string(keys[0])}, got)
	})

	t.Run("reverse seek on empty bucket is invalid", func(t *testing.T) {
		emptyDir, err := os.MkdirTemp("", "bolt-cursor-empty")
		require.NoError(t, err)
		defer os.RemoveAll(emptyDir)

		emptyStore, err := Open(emptyDir)
		require.NoError(t, err)
		defer emptyStore.Close()

		emptyTx, err := emptyStore.Begin(false)
		require.NoError(t, err)
		defer emptyTx.Rollback()

		cursor, err := emptyTx.Cursor(false)
		require.NoError(t, err)
		defer cursor.Close()

		require.NoError(t, cursor.Seek([]byte("any")))
		require.False(t, cursor.Valid())
	})
}
