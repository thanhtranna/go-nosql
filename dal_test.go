package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestDAL(t *testing.T) (*dal, func()) {
	fileName := getTempFileName()
	dal, err := newDal(fileName, &Options{
		pageSize: testPageSize,
	})
	require.NoError(t, err)

	cleanFunc := func() {
		err = dal.close()
		require.NoError(t, err)
		err = os.Remove(fileName)
		require.NoError(t, err)
	}

	return dal, cleanFunc
}

func TestCreateAndGetNode(t *testing.T) {
	dal, cleanFunc := createTestDAL(t)
	defer cleanFunc()

	items := []*Item{newItem([]byte("key1"), []byte("val1")), newItem([]byte("key2"), []byte("val2"))}
	var childNodes []pageNum

	expectedNode, err := dal.writeNode(NewNodeForSerialization(items, childNodes))
	require.NoError(t, err)

	actualNode, err := dal.getNode(expectedNode.pgNum)
	require.NoError(t, err)

	assert.Equal(t, expectedNode, actualNode)
}

func TestDeleteNode(t *testing.T) {
	dal, cleanFunc := createTestDAL(t)
	defer cleanFunc()

	var items []*Item
	var childNodes []pageNum

	node, err := dal.writeNode(NewNodeForSerialization(items, childNodes))
	require.NoError(t, err)
	assert.Equal(t, node.pgNum, dal.maxPage)

	dal.deleteNode(node.pgNum)

	assert.Equal(t, dal.releasedPages, []pageNum{node.pgNum})
	assert.Equal(t, node.pgNum, dal.maxPage)
}

func TestDeleteNodeAndReusePage(t *testing.T) {
	dal, cleanFunc := createTestDAL(t)
	defer cleanFunc()

	var items []*Item
	var childNodes []pageNum

	node, err := dal.writeNode(NewNodeForSerialization(items, childNodes))
	require.NoError(t, err)
	assert.Equal(t, node.pgNum, dal.maxPage)

	dal.deleteNode(node.pgNum)

	assert.Equal(t, dal.releasedPages, []pageNum{node.pgNum})
	assert.Equal(t, node.pgNum, dal.maxPage)

	newNode, err := dal.writeNode(NewNodeForSerialization(items, childNodes))
	require.NoError(t, err)
	assert.Equal(t, dal.releasedPages, []pageNum{})
	assert.Equal(t, newNode.pgNum, dal.maxPage)
}

func TestCreateDalWithNewFile(t *testing.T) {
	dal, cleanFunc := createTestDAL(t)
	defer cleanFunc()

	metaPage, err := dal.readMeta()
	require.NoError(t, err)

	freelistPageNum := pageNum(1)
	rootPageNum := pageNum(2)
	assert.Equal(t, freelistPageNum, metaPage.freelistPage)
	assert.Equal(t, rootPageNum, metaPage.root)

	assert.Equal(t, freelistPageNum, dal.freelistPage)
	assert.Equal(t, rootPageNum, dal.root)
}

func TestCreateDalWithExistingFile(t *testing.T) {
	// Make sure file exists
	_, err := os.Stat(getExpectedResultFileName(t.Name()))
	require.NoError(t, err)

	dal, cleanFunc := createTestDAL(t)
	defer cleanFunc()

	metaPage, err := dal.readMeta()
	require.NoError(t, err)

	freelistPageNum := pageNum(1)
	rootPageNum := pageNum(2)
	assert.Equal(t, freelistPageNum, metaPage.freelistPage)
	assert.Equal(t, rootPageNum, metaPage.root)

	assert.Equal(t, freelistPageNum, dal.freelistPage)
	assert.Equal(t, rootPageNum, dal.root)
}
