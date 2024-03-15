package com.example.box

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class BoxTest {
    private val boxDb = BoxDatabase()

    @BeforeEach
    fun setup() {
        boxDb.set("a", 1)
        boxDb.set("b", 2)
        boxDb.set("c", 1)
    }

    @AfterEach
    fun clearAll() {
        boxDb.clearAll()
    }

    @Test
    fun `base operation test`() {
        assertEquals(1, boxDb.get("a"))
        assertEquals(2, boxDb.count(1))

        boxDb.delete("c")
        assertEquals(1, boxDb.count(1))

    }

    @Test
    fun `transaction begin and rollback`() {
        boxDb.begin()
        assertEquals(1, boxDb.get("a"))

        boxDb.set("a", 2)
        assertEquals(2, boxDb.get("a"))

        boxDb.rollback()
        assertEquals(1, boxDb.get("a"))
    }

    @Test
    fun `transaction begin and delete and rollback and commit`() {
        boxDb.begin()
        assertEquals(1, boxDb.get("a"))

        boxDb.set("a", 2)
        assertEquals(2, boxDb.get("a"))

        boxDb.begin()
        boxDb.delete("a")
        assertNull(boxDb.get("a"))

        boxDb.rollback()
        assertEquals(2, boxDb.get("a"))

        boxDb.commit()
        assertEquals(2, boxDb.get("a"))
    }

    @Test
    fun `value count should decrease if key removed in transaction`() {
        boxDb.begin()
        assertEquals(2, boxDb.count(1))

        boxDb.delete("a")
        assertEquals(1, boxDb.count(1))

        boxDb.rollback()
        assertEquals(2, boxDb.count(1))
    }


    class BoxDatabase {
        private val table = mutableMapOf<String, Int?>()
        private val transactions = mutableListOf<TxState>()
        private val valueCountCache = mutableMapOf<Int, Int>()

        fun set(key: String, value: Int) {
            if (transactions.isEmpty()) {
                table[key] = value
            } else {
                setInTransaction(key, value)
            }
            increaseValueCountCache(value)
        }

        fun get(key: String): Int? {
            val keyInTransaction = transactions
                .any { it.keySet.contains(key) }
            return if (keyInTransaction) {
                getLastOperationValue(key)
            } else {
                table[key]
            }
        }

        fun delete(key: String) {
            if (transactions.isEmpty()) {
                decreaseCache(table[key])
                table.remove(key)
            } else {
                deleteInTransaction(key)
            }
        }

        fun count(value: Int): Int? {
            return valueCountCache[value]
        }

        /*
        *  Transaction operation
        * */

        fun begin() {
            transactions.add(TxState("BEGIN"))
        }

        fun commit() {
            if (transactions.isEmpty()) {
                println("NO TRANSACTION")
            }

            for (tx in transactions) {
                if (tx.operation != "BEGIN") {
                    table.putAll(tx.tmpTable)
                }
            }

            transactions.clear()
        }

        fun rollback() {
            if (transactions.isEmpty()) {
                println("NO TRANSACTION")
            } else {
                handleValueCountCacheRollback(transactions.last())
                transactions.removeLast()
            }
        }

        private fun deleteInTransaction(key: String) {
            decreaseCache(getLastOperationValue(key))
            transactions.add(TxState("DELETE").apply {
                prevTable[key] = getLastOperationValue(key)
                tmpTable[key] = null
                keySet.add(key)
            })
        }

        private fun setInTransaction(key: String, value: Int) {
            transactions.add(TxState("SET").apply {
                this.prevTable[key] = getLastOperationValue(key)
                this.tmpTable[key] = value
                this.keySet.add(key)
            })
        }

        private fun handleValueCountCacheRollback(last: TxState) {
            when (last.operation) {
                "BEGIN" -> {
                    return
                }

                "SET" -> {
                    for (tx in last.tmpTable)
                        decreaseCache(tx.value)
                }

                else -> {
                    for (tx in last.tmpTable)
                        if (tx.value != null) {
                            increaseValueCountCache(tx.value!!)
                        } else {
                            val value = last.prevTable[tx.key]
                            increaseValueCountCache(value!!)
                        }
                }
            }
        }

        private fun decreaseCache(value: Int?) {
            if (value != null) {
                valueCountCache[value] = (valueCountCache[value] ?: 0) - 1
            }
        }

        private fun increaseValueCountCache(value: Int) {
            valueCountCache[value] = (valueCountCache[value] ?: 0) + 1
        }

        private fun getLastOperationValue(key: String): Int? {
            for (i in transactions.lastIndex downTo 0) {
                if (transactions[i].keySet.contains(key)) {
                    return transactions[i].tmpTable[key]
                }
            }
            return table[key]
        }

        fun clearAll() {
            transactions.clear()
            table.clear()
        }

        private data class TxState(val operation: String) {
            val tmpTable = mutableMapOf<String, Int?>()
            val prevTable = mutableMapOf<String, Int?>()
            val keySet = mutableSetOf<String>()
        }
    }
}
