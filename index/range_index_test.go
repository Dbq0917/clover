package index

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ostafen/clover/v2/store"
	badgerstore "github.com/ostafen/clover/v2/store/badger"
	"github.com/ostafen/clover/v2/store/bbolt"
)

func rangeDocId(i int) string {
	return "00000000-0000-0000-0000-00000000000" + string(rune('0'+i))
}

func rangeStoreFactories() map[string]func(string) (store.Store, error) {
	return map[string]func(string) (store.Store, error){
		"badger": func(dir string) (store.Store, error) {
			return badgerstore.Open(dir)
		},
		"bbolt": func(dir string) (store.Store, error) {
			return bbolt.Open(dir)
		},
	}
}

func TestRangeIndexIterateRangeParity(t *testing.T) {
	// value 2 and 4 are repeated on purpose so that the Seek/skip logic has to
	// move across multiple keys sharing the same encoded value prefix.
	values := []float64{1, 2, 2, 3, 4, 4, 5}

	ranges := map[string]*Range{
		"closed":        {Start: float64(2), End: float64(4), StartIncluded: true, EndIncluded: true},
		"open":          {Start: float64(2), End: float64(4), StartIncluded: false, EndIncluded: false},
		"openStart":     {Start: nil, End: float64(4), StartIncluded: false, EndIncluded: true},
		"openStartExcl": {Start: nil, End: float64(4), StartIncluded: false, EndIncluded: false},
		"openEnd":       {Start: float64(2), End: nil, StartIncluded: true, EndIncluded: false},
		"openEndExcl":   {Start: float64(2), End: nil, StartIncluded: false, EndIncluded: false},
	}

	expected := map[string][]string{
		"closed":                {rangeDocId(1), rangeDocId(2), rangeDocId(3), rangeDocId(4), rangeDocId(5)},
		"closed:reverse":        {rangeDocId(5), rangeDocId(4), rangeDocId(3), rangeDocId(2), rangeDocId(1)},
		"open":                  {rangeDocId(3)},
		"open:reverse":          {rangeDocId(3)},
		"openStart":             {rangeDocId(0), rangeDocId(1), rangeDocId(2), rangeDocId(3), rangeDocId(4), rangeDocId(5)},
		"openStart:reverse":     {rangeDocId(5), rangeDocId(4), rangeDocId(3), rangeDocId(2), rangeDocId(1), rangeDocId(0)},
		"openStartExcl":         {rangeDocId(0), rangeDocId(1), rangeDocId(2), rangeDocId(3)},
		"openStartExcl:reverse": {rangeDocId(3), rangeDocId(2), rangeDocId(1), rangeDocId(0)},
		"openEnd":               {rangeDocId(1), rangeDocId(2), rangeDocId(3), rangeDocId(4), rangeDocId(5), rangeDocId(6)},
		"openEnd:reverse":       {rangeDocId(6), rangeDocId(5), rangeDocId(4), rangeDocId(3), rangeDocId(2), rangeDocId(1)},
		"openEndExcl":           {rangeDocId(3), rangeDocId(4), rangeDocId(5), rangeDocId(6)},
		"openEndExcl:reverse":   {rangeDocId(6), rangeDocId(5), rangeDocId(4), rangeDocId(3)},
	}

	for name, openStore := range rangeStoreFactories() {
		t.Run(name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "idx-range")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			s, err := openStore(dir)
			require.NoError(t, err)
			defer s.Close()

			tx, err := s.Begin(true)
			require.NoError(t, err)

			writeIdx := CreateIndex("coll", "field", SingleField, tx).(*rangeIndex)
			for i, v := range values {
				require.NoError(t, writeIdx.Add(rangeDocId(i), v, 0))
			}
			require.NoError(t, tx.Commit())

			rtx, err := s.Begin(false)
			require.NoError(t, err)
			defer rtx.Rollback()

			idx := CreateIndex("coll", "field", SingleField, rtx).(*rangeIndex)

			got := make(map[string][]string)
			for rangeName, vRange := range ranges {
				for _, reverse := range []bool{false, true} {
					key := rangeName
					if reverse {
						key += ":reverse"
					}

					var docIds []string
					require.NoError(t, idx.IterateRange(vRange, reverse, func(docId string) error {
						docIds = append(docIds, docId)
						return nil
					}))
					got[key] = docIds
				}
			}

			require.Equal(t, expected, got)
		})
	}
}

func TestRangeIndexIterateParity(t *testing.T) {
	for name, openStore := range rangeStoreFactories() {
		t.Run(name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "idx-iterate")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			s, err := openStore(dir)
			require.NoError(t, err)
			defer s.Close()

			tx, err := s.Begin(true)
			require.NoError(t, err)

			writeIdx := CreateIndex("coll", "field", SingleField, tx).(*rangeIndex)
			for i := 0; i < 5; i++ {
				require.NoError(t, writeIdx.Add(rangeDocId(i), float64(i), 0))
			}
			require.NoError(t, tx.Commit())

			rtx, err := s.Begin(false)
			require.NoError(t, err)
			defer rtx.Rollback()

			idx := CreateIndex("coll", "field", SingleField, rtx).(*rangeIndex)

			for _, reverse := range []bool{false, true} {
				var docIds []string
				require.NoError(t, idx.Iterate(reverse, func(docId string) error {
					docIds = append(docIds, docId)
					return nil
				}))

				want := []string{rangeDocId(0), rangeDocId(1), rangeDocId(2), rangeDocId(3), rangeDocId(4)}
				if reverse {
					for i, j := 0, len(want)-1; i < j; i, j = i+1, j-1 {
						want[i], want[j] = want[j], want[i]
					}
				}
				require.Equal(t, want, docIds)
			}
		})
	}
}
