package main

import (
	"bytes"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_AddSingle(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()
	collection, err := tx.CreateCollection(testCollectionName)
	require.NoError(t, err)

	value := createItem("0")
	err = collection.Put(value, value)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("0"), []pageNum{}))
	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pgNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	areTreesEqual(t, expectedCollection, collection)
}

func Test_RemoveFromRootSingleElement(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()
	collection, err := tx.CreateCollection(testCollectionName)
	require.NoError(t, err)

	value := createItem("0")
	err = collection.Put(value, value)
	require.NoError(t, err)

	err = collection.Remove(value)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDBAfterRemoval, cleanFuncAfterRemoval := createTestDB(t)
	defer cleanFuncAfterRemoval()

	expectedTxAfterRemoval := expectedDBAfterRemoval.WriteTx()

	expectedRootAfterRemoval := expectedTxAfterRemoval.writeNode(expectedTxAfterRemoval.newNode([]*Item{}, []pageNum{}))

	expectedCollectionAfterRemoval, err := expectedTxAfterRemoval.createCollection(newCollection(testCollectionName, expectedRootAfterRemoval.pgNum))

	err = expectedTxAfterRemoval.Commit()
	require.NoError(t, err)

	areTreesEqual(t, expectedCollectionAfterRemoval, collection)
}

func Test_AddMultiple(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()
	collection, err := tx.CreateCollection(testCollectionName)
	require.NoError(t, err)

	numOfElements := mockNumberOfElements
	for i := 0; i < numOfElements; i++ {
		val := createItem(strconv.Itoa(i))
		err = collection.Put(val, val)
		require.NoError(t, err)
	}
	err = tx.Commit()
	require.NoError(t, err)

	// Tree is balanced
	expected, expectedCleanFunc := createTestMockTree(t)
	defer expectedCleanFunc()
	areTreesEqual(t, expected, collection)
}

func Test_AddAndRebalanceSplit(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()
	child0 := tx.writeNode(tx.newNode(createItems("0", "1", "2", "3"), []pageNum{}))

	child1 := tx.writeNode(tx.newNode(createItems("5", "6", "7", "8"), []pageNum{}))

	root := tx.writeNode(tx.newNode(createItems("4"), []pageNum{child0.pgNum, child1.pgNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pgNum))
	require.NoError(t, err)

	val := createItem("9")
	err = collection.Put(val, val)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedTestDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	testTx := expectedTestDB.WriteTx()

	expectedChild0 := testTx.writeNode(testTx.newNode(createItems("0", "1", "2", "3"), []pageNum{}))

	expectedChild1 := testTx.writeNode(testTx.newNode(createItems("5", "6"), []pageNum{}))

	expectedChild2 := testTx.writeNode(testTx.newNode(createItems("8", "9"), []pageNum{}))

	expectedRoot := testTx.writeNode(testTx.newNode(createItems("4", "7"), []pageNum{expectedChild0.pgNum, expectedChild1.pgNum, expectedChild2.pgNum}))

	expectedCollection, err := testTx.createCollection(newCollection(testCollectionName, expectedRoot.pgNum))
	require.NoError(t, err)

	err = testTx.Commit()
	require.NoError(t, err)

	// Tree is balanced
	areTreesEqual(t, expectedCollection, collection)
}

func Test_SplitAndMerge(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()
	child0 := tx.writeNode(tx.newNode(createItems("0", "1", "2", "3"), []pageNum{}))

	child1 := tx.writeNode(tx.newNode(createItems("5", "6", "7", "8"), []pageNum{}))

	root := tx.writeNode(tx.newNode(createItems("4"), []pageNum{child0.pgNum, child1.pgNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pgNum))
	require.NoError(t, err)

	val := createItem("9")
	err = collection.Put(val, val)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedTestDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	testTx := expectedTestDB.WriteTx()

	expectedChild0 := testTx.writeNode(testTx.newNode(createItems("0", "1", "2", "3"), []pageNum{}))

	expectedChild1 := testTx.writeNode(testTx.newNode(createItems("5", "6"), []pageNum{}))

	expectedChild2 := testTx.writeNode(testTx.newNode(createItems("8", "9"), []pageNum{}))

	expectedRoot := testTx.writeNode(testTx.newNode(createItems("4", "7"), []pageNum{expectedChild0.pgNum, expectedChild1.pgNum, expectedChild2.pgNum}))

	expectedCollection, err := testTx.createCollection(newCollection(testCollectionName, expectedRoot.pgNum))
	require.NoError(t, err)

	// Tree is balanced
	areTreesEqual(t, expectedCollection, collection)

	err = testTx.Commit()
	require.NoError(t, err)

	removeTx := db.WriteTx()
	collection, err = removeTx.GetCollection(collection.name)
	require.NoError(t, err)

	err = collection.Remove(val)
	require.NoError(t, err)

	err = removeTx.Commit()
	require.NoError(t, err)

	expectedDBAfterRemoval, expectedDBCleanFunc := createTestDB(t)
	defer expectedDBCleanFunc()

	expectedTxAfterRemoval := expectedDBAfterRemoval.WriteTx()
	expectedChild0AfterRemoval := expectedTxAfterRemoval.writeNode(expectedTxAfterRemoval.newNode(createItems("0", "1", "2", "3"), []pageNum{}))

	expectedChild1AfterRemoval := expectedTxAfterRemoval.writeNode(expectedTxAfterRemoval.newNode(createItems("5", "6", "7", "8"), []pageNum{}))

	expectedRootAfterRemoval := expectedTxAfterRemoval.writeNode(expectedTxAfterRemoval.newNode(createItems("4"), []pageNum{expectedChild0AfterRemoval.pgNum, expectedChild1AfterRemoval.pgNum}))

	expectedCollectionAfterRemoval, err := expectedTxAfterRemoval.createCollection(newCollection(testCollectionName, expectedRootAfterRemoval.pgNum))
	require.NoError(t, err)

	err = expectedTxAfterRemoval.Commit()
	require.NoError(t, err)

	areTreesEqual(t, expectedCollectionAfterRemoval, collection)
}

func Test_RemoveFromRootWithoutRebalance(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()
	collection, err := tx.CreateCollection(testCollectionName)
	require.NoError(t, err)

	numOfElements := mockNumberOfElements
	for i := 0; i < numOfElements; i++ {
		val := createItem(strconv.Itoa(i))
		err = collection.Put(val, val)
		require.NoError(t, err)
	}

	// Remove an element
	err = collection.Remove(createItem("7"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTestTx := expectedDB.WriteTx()

	expectedChild0 := expectedTestTx.writeNode(expectedTestTx.newNode(createItems("0", "1"), []pageNum{}))

	expectedChild1 := expectedTestTx.writeNode(expectedTestTx.newNode(createItems("3", "4"), []pageNum{}))

	expectedChild2 := expectedTestTx.writeNode(expectedTestTx.newNode(createItems("6", "8", "9"), []pageNum{}))

	expectedRoot := expectedTestTx.writeNode(expectedTestTx.newNode(createItems("2", "5"), []pageNum{expectedChild0.pgNum, expectedChild1.pgNum, expectedChild2.pgNum}))

	expectedCollectionAfterRemoval, err := expectedTestTx.createCollection(newCollection(testCollectionName, expectedRoot.pgNum))
	require.NoError(t, err)

	err = expectedTestTx.Commit()
	require.NoError(t, err)

	areTreesEqual(t, expectedCollectionAfterRemoval, collection)
}

func Test_RemoveFromRootAndRotateLeft(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child0 := tx.writeNode(tx.newNode(createItems("0", "1"), []pageNum{}))

	child1 := tx.writeNode(tx.newNode(createItems("3", "4"), []pageNum{}))

	child2 := tx.writeNode(tx.newNode(createItems("6", "7", "8"), []pageNum{}))

	root := tx.writeNode(tx.newNode(createItems("2", "5"), []pageNum{child0.pgNum, child1.pgNum, child2.pgNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pgNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("5"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("0", "1"), []pageNum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("3", "4"), []pageNum{}))

	expectedChild2 := expectedTx.writeNode(expectedTx.newNode(createItems("7", "8"), []pageNum{}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("2", "6"), []pageNum{expectedChild0.pgNum, expectedChild1.pgNum, expectedChild2.pgNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pgNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	areTreesEqual(t, expectedCollection, collection)
}

func Test_RemoveFromRootAndRotateRight(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child0 := tx.writeNode(tx.newNode(createItems("0", "1", "2"), []pageNum{}))

	child1 := tx.writeNode(tx.newNode(createItems("4", "5"), []pageNum{}))

	child2 := tx.writeNode(tx.newNode(createItems("7", "8"), []pageNum{}))

	root := tx.writeNode(tx.newNode(createItems("3", "6"), []pageNum{child0.pgNum, child1.pgNum, child2.pgNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pgNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("6"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("0", "1"), []pageNum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("3", "4"), []pageNum{}))

	expectedChild2 := expectedTx.writeNode(expectedTx.newNode(createItems("7", "8"), []pageNum{}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("2", "5"), []pageNum{expectedChild0.pgNum, expectedChild1.pgNum, expectedChild2.pgNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pgNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	areTreesEqual(t, expectedCollection, collection)
}

// Test_RemoveFromRootAndRebalanceMergeToUnbalanced tests when the unbalanced node is the most left one so the
// merge has to happen from the right node into the unbalanced node
func Test_RemoveFromRootAndRebalanceMergeToUnbalanced(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child0 := tx.writeNode(tx.newNode(createItems("0", "1"), []pageNum{}))

	child1 := tx.writeNode(tx.newNode(createItems("3", "4"), []pageNum{}))

	child2 := tx.writeNode(tx.newNode(createItems("6", "7"), []pageNum{}))

	root := tx.writeNode(tx.newNode(createItems("2", "5"), []pageNum{child0.pgNum, child1.pgNum, child2.pgNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pgNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("2"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("0", "1", "3", "4"), []pageNum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("6", "7"), []pageNum{}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("5"), []pageNum{expectedChild0.pgNum, expectedChild1.pgNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pgNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	areTreesEqual(t, expectedCollection, collection)
}

// Test_RemoveFromRootAndRebalanceMergeFromUnbalanced tests when the unbalanced node is not the most left one so the
// merge has to happen from the unbalanced node to the node left to it
func Test_RemoveFromRootAndRebalanceMergeFromUnbalanced(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child0 := tx.writeNode(tx.newNode(createItems("0", "1"), []pageNum{}))

	child1 := tx.writeNode(tx.newNode(createItems("3", "4"), []pageNum{}))

	child2 := tx.writeNode(tx.newNode(createItems("6", "7"), []pageNum{}))

	root := tx.writeNode(tx.newNode(createItems("2", "5"), []pageNum{child0.pgNum, child1.pgNum, child2.pgNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pgNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("5"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("0", "1", "2", "3"), []pageNum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("6", "7"), []pageNum{}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("4"), []pageNum{expectedChild0.pgNum, expectedChild1.pgNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pgNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	// Remove an element
	areTreesEqual(t, expectedCollection, collection)
}

func Test_RemoveFromInnerNodeAndRotateLeft(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child00 := tx.writeNode(tx.newNode(createItems("0", "1"), []pageNum{}))

	child01 := tx.writeNode(tx.newNode(createItems("3", "4"), []pageNum{}))

	child02 := tx.writeNode(tx.newNode(createItems("6", "7"), []pageNum{}))

	child0 := tx.writeNode(tx.newNode(createItems("2", "5"), []pageNum{child00.pgNum, child01.pgNum, child02.pgNum}))

	child10 := tx.writeNode(tx.newNode(createItems("9", "a"), []pageNum{}))

	child11 := tx.writeNode(tx.newNode(createItems("c", "d"), []pageNum{}))

	child12 := tx.writeNode(tx.newNode(createItems("f", "g"), []pageNum{}))

	child13 := tx.writeNode(tx.newNode(createItems("i", "j"), []pageNum{}))

	child1 := tx.writeNode(tx.newNode(createItems("b", "e", "h"), []pageNum{child10.pgNum, child11.pgNum, child12.pgNum, child13.pgNum}))

	root := tx.writeNode(tx.newNode(createItems("8"), []pageNum{child0.pgNum, child1.pgNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pgNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("5"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild00 := expectedTx.writeNode(expectedTx.newNode(createItems("0", "1", "2", "3"), []pageNum{}))

	expectedChild01 := expectedTx.writeNode(expectedTx.newNode(createItems("6", "7"), []pageNum{}))

	expectedChild02 := expectedTx.writeNode(expectedTx.newNode(createItems("9", "a"), []pageNum{}))

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("4", "8"), []pageNum{expectedChild00.pgNum, expectedChild01.pgNum, expectedChild02.pgNum}))

	expectedChild10 := expectedTx.writeNode(expectedTx.newNode(createItems("c", "d"), []pageNum{}))

	expectedChild11 := expectedTx.writeNode(expectedTx.newNode(createItems("f", "g"), []pageNum{}))

	expectedChild12 := expectedTx.writeNode(expectedTx.newNode(createItems("i", "j"), []pageNum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("e", "h"), []pageNum{expectedChild10.pgNum, expectedChild11.pgNum, expectedChild12.pgNum}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("b"), []pageNum{expectedChild0.pgNum, expectedChild1.pgNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pgNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	// Remove an element
	areTreesEqual(t, expectedCollection, collection)
}

func Test_RemoveFromInnerNodeAndRotateRight(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child00 := tx.writeNode(tx.newNode(createItems("0", "1"), []pageNum{}))

	child01 := tx.writeNode(tx.newNode(createItems("3", "4"), []pageNum{}))

	child02 := tx.writeNode(tx.newNode(createItems("6", "7"), []pageNum{}))

	child03 := tx.writeNode(tx.newNode(createItems("9", "a"), []pageNum{}))

	child0 := tx.writeNode(tx.newNode(createItems("2", "5", "8"), []pageNum{child00.pgNum, child01.pgNum, child02.pgNum, child03.pgNum}))

	child10 := tx.writeNode(tx.newNode(createItems("c", "d"), []pageNum{}))

	child11 := tx.writeNode(tx.newNode(createItems("f", "g"), []pageNum{}))

	child12 := tx.writeNode(tx.newNode(createItems("i", "j"), []pageNum{}))

	child1 := tx.writeNode(tx.newNode(createItems("e", "h"), []pageNum{child10.pgNum, child11.pgNum, child12.pgNum}))

	root := tx.writeNode(tx.newNode(createItems("b"), []pageNum{child0.pgNum, child1.pgNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pgNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("e"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild00 := expectedTx.writeNode(expectedTx.newNode(createItems("0", "1"), []pageNum{}))

	expectedChild01 := expectedTx.writeNode(expectedTx.newNode(createItems("3", "4"), []pageNum{}))

	expectedChild02 := expectedTx.writeNode(expectedTx.newNode(createItems("6", "7"), []pageNum{}))

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("2", "5"), []pageNum{expectedChild00.pgNum, expectedChild01.pgNum, expectedChild02.pgNum}))

	expectedChild10 := expectedTx.writeNode(expectedTx.newNode(createItems("9", "a"), []pageNum{}))

	expectedChild11 := expectedTx.writeNode(expectedTx.newNode(createItems("c", "d", "f", "g"), []pageNum{}))

	expectedChild12 := expectedTx.writeNode(expectedTx.newNode(createItems("i", "j"), []pageNum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("b", "h"), []pageNum{expectedChild10.pgNum, expectedChild11.pgNum, expectedChild12.pgNum}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("8"), []pageNum{expectedChild0.pgNum, expectedChild1.pgNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pgNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	// Remove an element
	areTreesEqual(t, expectedCollection, collection)
}

func Test_RemoveFromInnerNodeAndUnion(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child00 := tx.writeNode(tx.newNode(createItems("0", "1"), []pageNum{}))

	child01 := tx.writeNode(tx.newNode(createItems("3", "4"), []pageNum{}))

	child02 := tx.writeNode(tx.newNode(createItems("6", "7"), []pageNum{}))

	child0 := tx.writeNode(tx.newNode(createItems("2", "5"), []pageNum{child00.pgNum, child01.pgNum, child02.pgNum}))

	child10 := tx.writeNode(tx.newNode(createItems("9", "a"), []pageNum{}))

	child11 := tx.writeNode(tx.newNode(createItems("c", "d"), []pageNum{}))

	child12 := tx.writeNode(tx.newNode(createItems("f", "g"), []pageNum{}))

	child1 := tx.writeNode(tx.newNode(createItems("b", "e"), []pageNum{child10.pgNum, child11.pgNum, child12.pgNum}))

	root := tx.writeNode(tx.newNode(createItems("8"), []pageNum{child0.pgNum, child1.pgNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pgNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("2"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("0", "1", "3", "4"), []pageNum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("6", "7"), []pageNum{}))

	expectedChild2 := expectedTx.writeNode(expectedTx.newNode(createItems("9", "a"), []pageNum{}))

	expectedChild3 := expectedTx.writeNode(expectedTx.newNode(createItems("c", "d"), []pageNum{}))

	expectedChild4 := expectedTx.writeNode(expectedTx.newNode(createItems("f", "g"), []pageNum{}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("5", "8", "b", "e"), []pageNum{expectedChild0.pgNum, expectedChild1.pgNum, expectedChild2.pgNum, expectedChild3.pgNum, expectedChild4.pgNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pgNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	// Remove an element
	areTreesEqual(t, expectedCollection, collection)
}

func Test_RemoveFromLeafAndRotateLeft(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child00 := tx.writeNode(tx.newNode(createItems("0", "1"), []pageNum{}))

	child01 := tx.writeNode(tx.newNode(createItems("3", "4", "5"), []pageNum{}))

	child02 := tx.writeNode(tx.newNode(createItems("7", "8"), []pageNum{}))

	child0 := tx.writeNode(tx.newNode(createItems("2", "6"), []pageNum{child00.pgNum, child01.pgNum, child02.pgNum}))

	child10 := tx.writeNode(tx.newNode(createItems("a", "b"), []pageNum{}))

	child11 := tx.writeNode(tx.newNode(createItems("d", "e"), []pageNum{}))

	child12 := tx.writeNode(tx.newNode(createItems("g", "h"), []pageNum{}))

	child1 := tx.writeNode(tx.newNode(createItems("c", "f"), []pageNum{child10.pgNum, child11.pgNum, child12.pgNum}))

	root := tx.writeNode(tx.newNode(createItems("9"), []pageNum{child0.pgNum, child1.pgNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pgNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("1"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild00 := expectedTx.writeNode(expectedTx.newNode(createItems("0", "2"), []pageNum{}))

	expectedChild01 := expectedTx.writeNode(expectedTx.newNode(createItems("4", "5"), []pageNum{}))

	expectedChild02 := expectedTx.writeNode(expectedTx.newNode(createItems("7", "8"), []pageNum{}))

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("3", "6"), []pageNum{expectedChild00.pgNum, expectedChild01.pgNum, expectedChild02.pgNum}))

	expectedChild10 := expectedTx.writeNode(expectedTx.newNode(createItems("a", "b"), []pageNum{}))

	expectedChild11 := expectedTx.writeNode(expectedTx.newNode(createItems("d", "e"), []pageNum{}))

	expectedChild12 := expectedTx.writeNode(expectedTx.newNode(createItems("g", "h"), []pageNum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("c", "f"), []pageNum{expectedChild10.pgNum, expectedChild11.pgNum, expectedChild12.pgNum}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("9"), []pageNum{expectedChild0.pgNum, expectedChild1.pgNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pgNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	// Remove an element
	areTreesEqual(t, expectedCollection, collection)
}

func Test_RemoveFromLeafAndRotateRight(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child00 := tx.writeNode(tx.newNode(createItems("0", "1"), []pageNum{}))

	child01 := tx.writeNode(tx.newNode(createItems("3", "4", "5"), []pageNum{}))

	child02 := tx.writeNode(tx.newNode(createItems("7", "8"), []pageNum{}))

	child0 := tx.writeNode(tx.newNode(createItems("2", "6"), []pageNum{child00.pgNum, child01.pgNum, child02.pgNum}))

	child10 := tx.writeNode(tx.newNode(createItems("a", "b"), []pageNum{}))

	child11 := tx.writeNode(tx.newNode(createItems("d", "e"), []pageNum{}))

	child12 := tx.writeNode(tx.newNode(createItems("g", "h"), []pageNum{}))

	child1 := tx.writeNode(tx.newNode(createItems("c", "f"), []pageNum{child10.pgNum, child11.pgNum, child12.pgNum}))

	root := tx.writeNode(tx.newNode(createItems("9"), []pageNum{child0.pgNum, child1.pgNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pgNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("8"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild00 := expectedTx.writeNode(expectedTx.newNode(createItems("0", "1"), []pageNum{}))

	expectedChild01 := expectedTx.writeNode(expectedTx.newNode(createItems("3", "4"), []pageNum{}))

	expectedChild02 := expectedTx.writeNode(expectedTx.newNode(createItems("6", "7"), []pageNum{}))

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("2", "5"), []pageNum{expectedChild00.pgNum, expectedChild01.pgNum, expectedChild02.pgNum}))

	expectedChild10 := expectedTx.writeNode(expectedTx.newNode(createItems("a", "b"), []pageNum{}))

	expectedChild11 := expectedTx.writeNode(expectedTx.newNode(createItems("d", "e"), []pageNum{}))

	expectedChild12 := expectedTx.writeNode(expectedTx.newNode(createItems("g", "h"), []pageNum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("c", "f"), []pageNum{expectedChild10.pgNum, expectedChild11.pgNum, expectedChild12.pgNum}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("9"), []pageNum{expectedChild0.pgNum, expectedChild1.pgNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pgNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	// Remove an element
	areTreesEqual(t, expectedCollection, collection)
}

func Test_RemoveFromLeafAndUnion(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child00 := tx.writeNode(tx.newNode(createItems("0", "1"), []pageNum{}))

	child01 := tx.writeNode(tx.newNode(createItems("3", "4"), []pageNum{}))

	child02 := tx.writeNode(tx.newNode(createItems("6", "7"), []pageNum{}))

	child0 := tx.writeNode(tx.newNode(createItems("2", "5"), []pageNum{child00.pgNum, child01.pgNum, child02.pgNum}))

	child10 := tx.writeNode(tx.newNode(createItems("9", "a"), []pageNum{}))

	child11 := tx.writeNode(tx.newNode(createItems("c", "d"), []pageNum{}))

	child12 := tx.writeNode(tx.newNode(createItems("f", "g"), []pageNum{}))

	child1 := tx.writeNode(tx.newNode(createItems("b", "e"), []pageNum{child10.pgNum, child11.pgNum, child12.pgNum}))

	root := tx.writeNode(tx.newNode(createItems("8"), []pageNum{child0.pgNum, child1.pgNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pgNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("0"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("1", "2", "3", "4"), []pageNum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("6", "7"), []pageNum{}))

	expectedChild2 := expectedTx.writeNode(expectedTx.newNode(createItems("9", "a"), []pageNum{}))

	expectedChild3 := expectedTx.writeNode(expectedTx.newNode(createItems("c", "d"), []pageNum{}))

	expectedChild4 := expectedTx.writeNode(expectedTx.newNode(createItems("f", "g"), []pageNum{}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("5", "8", "b", "e"), []pageNum{expectedChild0.pgNum, expectedChild1.pgNum, expectedChild2.pgNum, expectedChild3.pgNum, expectedChild4.pgNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pgNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	// Remove an element
	areTreesEqual(t, expectedCollection, collection)
}

func Test_FindNode(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child00 := tx.writeNode(tx.newNode(createItems("0", "1"), []pageNum{}))

	child01 := tx.writeNode(tx.newNode(createItems("3", "4"), []pageNum{}))

	child02 := tx.writeNode(tx.newNode(createItems("6", "7"), []pageNum{}))

	child0 := tx.writeNode(tx.newNode(createItems("2", "5"), []pageNum{child00.pgNum, child01.pgNum, child02.pgNum}))

	child10 := tx.writeNode(tx.newNode(createItems("9", "a"), []pageNum{}))

	child11 := tx.writeNode(tx.newNode(createItems("c", "d"), []pageNum{}))

	child12 := tx.writeNode(tx.newNode(createItems("f", "g"), []pageNum{}))

	child1 := tx.writeNode(tx.newNode(createItems("b", "e"), []pageNum{child10.pgNum, child11.pgNum, child12.pgNum}))

	root := tx.writeNode(tx.newNode(createItems("8"), []pageNum{child0.pgNum, child1.pgNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pgNum))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Item found
	expectedVal := createItem("c")
	expectedItem := newItem(expectedVal, expectedVal)
	item, err := collection.Find(expectedVal)
	require.NoError(t, err)
	assert.Equal(t, expectedItem, item)

	// Item not found
	expectedVal = createItem("h")
	item, err = collection.Find(expectedVal)
	require.NoError(t, err)
	assert.Nil(t, item)
}

func Test_UpdateNode(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child00 := tx.writeNode(tx.newNode(createItems("0", "1"), []pageNum{}))

	child01 := tx.writeNode(tx.newNode(createItems("3", "4"), []pageNum{}))

	child02 := tx.writeNode(tx.newNode(createItems("6", "7"), []pageNum{}))

	child0 := tx.writeNode(tx.newNode(createItems("2", "5"), []pageNum{child00.pgNum, child01.pgNum, child02.pgNum}))

	child10 := tx.writeNode(tx.newNode(createItems("9", "a"), []pageNum{}))

	child11 := tx.writeNode(tx.newNode(createItems("c", "d"), []pageNum{}))

	child12 := tx.writeNode(tx.newNode(createItems("f", "g"), []pageNum{}))

	child1 := tx.writeNode(tx.newNode(createItems("b", "e"), []pageNum{child10.pgNum, child11.pgNum, child12.pgNum}))

	root := tx.writeNode(tx.newNode(createItems("8"), []pageNum{child0.pgNum, child1.pgNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pgNum))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	tx2 := db.WriteTx()
	collection, err = tx2.GetCollection(collection.name)
	require.NoError(t, err)

	// Item found
	expectedVal := createItem("c")
	expectedItem := newItem(expectedVal, expectedVal)
	item, err := collection.Find(expectedVal)
	require.NoError(t, err)
	assert.Equal(t, expectedItem, item)

	// Item updated successfully
	newvalue := createItem("f")
	err = collection.Put(expectedVal, newvalue)
	require.NoError(t, err)

	item, err = collection.Find(expectedVal)
	require.NoError(t, err)
	assert.Equal(t, newvalue, item.value)

	err = tx2.Commit()
	require.NoError(t, err)
}

func TestSerializeWithoutChildNodes(t *testing.T) {
	items := []*Item{newItem([]byte("key1"), []byte("val1")), newItem([]byte("key2"), []byte("val2"))}
	var childNodes []pageNum
	node := &Node{
		items:      items,
		childNodes: childNodes,
	}

	actual := node.serialize(make([]byte, testPageSize, testPageSize))

	expectedPage, err := os.ReadFile(getExpectedResultFileName(t.Name()))
	require.NoError(t, err)
	assert.Equal(t, 0, bytes.Compare(actual, expectedPage))
}

func TestDeserializeWithoutChildNodes(t *testing.T) {
	page, err := os.ReadFile(getExpectedResultFileName(t.Name()))
	require.NoError(t, err)

	actualNode := NewEmptyNode()
	actualNode.deserialize(page)

	items := []*Item{newItem([]byte("key1"), []byte("val1")), newItem([]byte("key2"), []byte("val2"))}
	var childNodes []pageNum
	expectedNode := &Node{
		items:      items,
		childNodes: childNodes,
	}

	assert.Equal(t, expectedNode, actualNode)
}

func TestSerializeWithChildNodes(t *testing.T) {
	items := []*Item{newItem([]byte("key1"), []byte("val1")), newItem([]byte("key2"), []byte("val2"))}
	childNodes := []pageNum{1, 2, 3}
	node := &Node{
		items:      items,
		childNodes: childNodes,
	}

	actual := node.serialize(make([]byte, testPageSize, testPageSize))

	expectedPage, err := os.ReadFile(getExpectedResultFileName(t.Name()))
	require.NoError(t, err)
	assert.Equal(t, 0, bytes.Compare(actual, expectedPage))
}

func TestDeserializeWithChildNodes(t *testing.T) {
	page, err := os.ReadFile(getExpectedResultFileName(t.Name()))
	require.NoError(t, err)

	items := []*Item{newItem([]byte("key1"), []byte("val1")), newItem([]byte("key2"), []byte("val2"))}
	childNodes := []pageNum{1, 2, 3}
	expectedNode := &Node{
		items:      items,
		childNodes: childNodes,
	}

	actualNode := NewEmptyNode()
	actualNode.deserialize(page)
	assert.Equal(t, expectedNode, actualNode)
}
