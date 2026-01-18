const pool = require("../../database");
const pool2 = require("../../databasePool");

function queryAsync(conn, sql, params) {
    return new Promise((resolve, reject) => {
        conn.query(sql, params, (err, results) => {
            if (err) return reject(err);
            resolve(results);
        });
    });
}

// Helper function to calculate bill totals after item deletion
function calculateBillTotals(bill, items) {
    const newTotal = items.reduce((sum, item) => sum + (parseFloat(item.price) || 0), 0);
    let discount = 0;

    if (bill.discountType === "percentage") {
        discount = (newTotal * (parseFloat(bill.discountValue) || 0)) / 100;
    } else if (bill.discountType === "fixed") {
        const fixedDiscount = parseFloat(bill.discountValue) || 0;
        discount = fixedDiscount > newTotal ? 0 : fixedDiscount;
    }

    return {
        totalAmount: newTotal,
        totalDiscount: discount,
        settledAmount: Math.max(0, newTotal - discount) // Prevent negative
    };
}

// Helper function to validate input
function validateSettleInput(fromDate, toDate, settleType, settleValue, firmId) {
    if (!fromDate || !toDate || !settleType || settleValue === undefined || settleValue === null || !firmId) {
        throw new Error("Missing required fields: fromDate, toDate, settleType, settleValue, firmId");
    }

    if (!['percentage', 'fixed'].includes(settleType)) {
        throw new Error("settleType must be 'percentage' or 'fixed'");
    }

    const numValue = parseFloat(settleValue);
    if (isNaN(numValue) || numValue < 0) {
        throw new Error("settleValue must be a non-negative number");
    }

    if (settleType === 'percentage' && (numValue > 100 || numValue < 0)) {
        throw new Error("Percentage must be between 0 and 100");
    }

    // Validate dates
    const fromDateObj = new Date(fromDate);
    const toDateObj = new Date(toDate);
    if (isNaN(fromDateObj.getTime()) || isNaN(toDateObj.getTime())) {
        throw new Error("Invalid date format");
    }
    if (fromDateObj > toDateObj) {
        throw new Error("fromDate must be before or equal to toDate");
    }

    return true;
}

const dryRunSettleBills = (req, res) => {
    const { fromDate, toDate, settleType, settleValue, firmId } = req.body;

    // Validate input
    try {
        validateSettleInput(fromDate, toDate, settleType, settleValue, firmId);
    } catch (validationError) {
        return res.status(400).json({ message: validationError.message });
    }

    pool2.getConnection(async (err, conn) => {
        if (err) {
            console.error('DB connection error:', err);
            return res.status(500).json({ message: "DB connection error" });
        }

        try {
            // 1. Fetch bills (no transaction needed for dry-run - only SELECT queries)
            const bills = await queryAsync(
                conn,
                `SELECT * FROM billing_data 
                 WHERE firmId = ? AND billDate BETWEEN ? AND ? 
                 ORDER BY totalAmount DESC`,
                [firmId, fromDate, toDate]
            );

            if (!bills.length) {
                conn.release();
                return res.json({ message: "No bills found in date range" });
            }

            // 2. Fetch all items in ONE query (fix N+1 problem)
            const billIds = bills.map(b => b.billId);
            if (billIds.length === 0) {
                conn.release();
                return res.json({ message: "No bills found" });
            }

            // Generate placeholders for IN clause (mysql library requires this)
            const placeholders = billIds.map(() => '?').join(',');
            const allItems = await queryAsync(
                conn,
                `SELECT * FROM billing_billWiseItem_data 
                 WHERE billId IN (${placeholders}) 
                 ORDER BY billId, price DESC`,
                billIds
            );

            // Group items by billId
            const itemsByBillId = {};
            allItems.forEach(item => {
                if (!itemsByBillId[item.billId]) {
                    itemsByBillId[item.billId] = [];
                }
                itemsByBillId[item.billId].push(item);
            });

            // Attach items to bills
            bills.forEach(bill => {
                bill.items = itemsByBillId[bill.billId] || [];
            });

            // 3. Calculate totals and target amount
            const originalTotal = bills.reduce((sum, b) => sum + parseFloat(b.totalAmount || 0), 0);
            const numSettleValue = parseFloat(settleValue);
            const targetAmount = settleType === "percentage"
                ? (originalTotal * numSettleValue) / 100
                : numSettleValue;

            // 4. Handle edge cases
            if (targetAmount >= originalTotal) {
                conn.release();
                return res.json({
                    mode: "dry-run",
                    originalTotal,
                    targetAmount,
                    finalTotal: originalTotal,
                    updatedBillsCount: 0,
                    message: "Target amount is greater than or equal to original total. No items need to be deleted.",
                    summary: { iterations: 0, totalDeletedItems: 0 },
                    deletedItemsByBill: {},
                    updatedBills: bills.map(b => ({
                        billId: b.billId,
                        totalAmount: parseFloat(b.totalAmount || 0),
                        totalDiscount: parseFloat(b.totalDiscount || 0),
                        settledAmount: parseFloat(b.settledAmount || 0),
                        remainingItems: (b.items || []).length
                    }))
                });
            }

            // Check if any bills have multiple items
            const billsWithMultipleItems = bills.filter(b => b.items && b.items.length > 1);
            if (billsWithMultipleItems.length === 0) {
                conn.release();
                return res.json({
                    mode: "dry-run",
                    originalTotal,
                    targetAmount,
                    finalTotal: originalTotal,
                    updatedBillsCount: 0,
                    message: "No bills have multiple items. Cannot delete items without leaving bills empty.",
                    summary: { iterations: 0, totalDeletedItems: 0 },
                    deletedItemsByBill: {},
                    updatedBills: bills.map(b => ({
                        billId: b.billId,
                        totalAmount: parseFloat(b.totalAmount || 0),
                        totalDiscount: parseFloat(b.totalDiscount || 0),
                        settledAmount: parseFloat(b.settledAmount || 0),
                        remainingItems: (b.items || []).length
                    }))
                });
            }

            // 5. Simulate deletions with safety limit
            const MAX_ITERATIONS = 10000; // Safety limit to prevent infinite loops
            let currentTotal = originalTotal;
            const deletedItemsByBill = {};
            let iterations = 0;

            while (currentTotal > targetAmount && iterations < MAX_ITERATIONS) {
                let deleted = false;

                for (const bill of bills) {
                    if (bill.items && bill.items.length > 1) {
                        const item = bill.items[0]; // highest priced item
                        bill.items.shift(); // remove from array

                        // Track deleted items by bill
                        if (!deletedItemsByBill[bill.billId]) {
                            deletedItemsByBill[bill.billId] = {
                                billDetails: {
                                    billId: bill.billId,
                                    billNumber: bill.billNumber,
                                    billDate: bill.billDate,
                                    originalTotal: parseFloat(bill.totalAmount || 0)
                                },
                                items: []
                            };
                        }
                        deletedItemsByBill[bill.billId].items.push(item);

                        // Recalculate bill totals using helper function
                        const totals = calculateBillTotals(bill, bill.items);
                        bill.totalAmount = totals.totalAmount;
                        bill.totalDiscount = totals.totalDiscount;
                        bill.settledAmount = totals.settledAmount;

                        currentTotal -= parseFloat(item.price || 0);
                        deleted = true;
                        break; // only one item per iteration
                    }
                }

                if (!deleted) break; // no more items can be deleted
                iterations++;
            }

            if (iterations >= MAX_ITERATIONS) {
                console.warn(`Reached max iterations (${MAX_ITERATIONS}). Target may not be achievable.`);
            }

            // 6. Build summary statistics
            const allDeleted = Object.values(deletedItemsByBill).flatMap(b => b.items);
            let summary = {};
            if (allDeleted.length > 0) {
                const prices = allDeleted.map(d => parseFloat(d.price || 0)).filter(p => !isNaN(p));
                if (prices.length > 0) {
                    summary = {
                        iterations,
                        totalDeletedItems: allDeleted.length,
                        largestDeletedItem: Math.max(...prices),
                        smallestDeletedItem: Math.min(...prices),
                        averageDeletedValue: (prices.reduce((a, b) => a + b, 0) / prices.length).toFixed(2),
                        totalDeletedValue: prices.reduce((a, b) => a + b, 0).toFixed(2)
                    };
                } else {
                    summary = { iterations, totalDeletedItems: allDeleted.length };
                }
            } else {
                summary = { iterations, totalDeletedItems: 0 };
            }

            // 7. Build response (optimized - only send necessary data)
            const updatedBillsCount = Object.keys(deletedItemsByBill).length;
            conn.release();
            res.json({
                mode: "dry-run",
                originalTotal: originalTotal.toFixed(2),
                targetAmount: targetAmount.toFixed(2),
                finalTotal: currentTotal.toFixed(2),
                amountReduction: (originalTotal - currentTotal).toFixed(2),
                updatedBillsCount: updatedBillsCount,
                summary,
                deletedItemsByBill: Object.entries(deletedItemsByBill).map(([billId, data]) => ({
                    billId,
                    billDetails: data.billDetails,
                    deletedItems: data.items,
                    deletedItemsCount: data.items.length,
                    deletedItemsValue: data.items.reduce((sum, item) => sum + parseFloat(item.price || 0), 0).toFixed(2)
                })),
                updatedBills: bills.map(b => ({
                    billId: b.billId,
                    totalAmount: parseFloat(b.totalAmount || 0).toFixed(2),
                    totalDiscount: parseFloat(b.totalDiscount || 0).toFixed(2),
                    settledAmount: parseFloat(b.settledAmount || 0).toFixed(2),
                    remainingItems: (b.items || []).length
                }))
            });

        } catch (error) {
            console.error('Error in dryRunSettleBills:', error);
            conn.release();
            res.status(500).json({
                message: "Internal server error",
                error: process.env.NODE_ENV === 'development' ? error.message : undefined
            });
        }
    });
};

const settleBills = (req, res) => {
    const { fromDate, toDate, settleType, settleValue, firmId } = req.body;

    // Validate input
    try {
        validateSettleInput(fromDate, toDate, settleType, settleValue, firmId);
    } catch (validationError) {
        return res.status(400).json({ message: validationError.message });
    }

    pool2.getConnection(async (err, conn) => {
        if (err) {
            console.error("DB connection error:", err);
            return res.status(500).json({ message: "DB connection error" });
        }
        try {
            await queryAsync(conn, "START TRANSACTION");

            // 0. Insert into temp_test_data
            const ttdId = `ttd_${Date.now()}`;
            await queryAsync(
                conn,
                `INSERT INTO temp_test_data(ttdId, startDate, endDate) VALUES(?, ?, ?)`,
                [ttdId, fromDate, toDate]
            );

            // 1. Fetch bills
            const bills = await queryAsync(
                conn,
                `SELECT * FROM billing_data 
                 WHERE firmId = ? AND billDate BETWEEN ? AND ?
                 ORDER BY totalAmount DESC`,
                [firmId, fromDate, toDate]
            );

            if (!bills.length) {
                await queryAsync(conn, "ROLLBACK");
                res.status(200).json({
                    message: "No bills found in date range",
                    statusCode: 200
                });
                return;
            }

            // 2. Fetch all items in ONE query (fix N+1 problem)
            const billIds = bills.map(b => b.billId);
            if (billIds.length === 0) {
                await queryAsync(conn, "ROLLBACK");
                res.status(200).json({
                    message: "No bills found",
                    statusCode: 200
                });
                return;
            }

            // Generate placeholders for IN clause (mysql library requires this)
            const placeholders = billIds.map(() => '?').join(',');
            const allItems = await queryAsync(
                conn,
                `SELECT * FROM billing_billWiseItem_data 
                 WHERE billId IN (${placeholders}) 
                 ORDER BY billId, price DESC`,
                billIds
            );

            // Group items by billId
            const itemsByBillId = {};
            allItems.forEach(item => {
                if (!itemsByBillId[item.billId]) {
                    itemsByBillId[item.billId] = [];
                }
                itemsByBillId[item.billId].push(item);
            });

            // Attach items to bills
            bills.forEach(bill => {
                bill.items = itemsByBillId[bill.billId] || [];
            });

            // 3. Calculate totals and target amount
            const originalTotal = bills.reduce((sum, b) => sum + parseFloat(b.totalAmount || 0), 0);
            const numSettleValue = parseFloat(settleValue);
            const targetAmount = settleType === "percentage"
                ? (originalTotal * numSettleValue) / 100
                : numSettleValue;

            // 4. Handle edge cases
            if (targetAmount >= originalTotal) {
                await queryAsync(conn, "ROLLBACK");
                res.status(200).json({
                    message: "Target amount is greater than or equal to original total. No items need to be deleted.",
                    statusCode: 200
                });
                return;
            }

            // Check if any bills have multiple items
            const billsWithMultipleItems = bills.filter(b => b.items && b.items.length > 1);
            if (billsWithMultipleItems.length === 0) {
                await queryAsync(conn, "ROLLBACK");
                res.status(400).json({
                    message: "No bills have multiple items. Cannot delete items without leaving bills empty.",
                    statusCode: 400
                });
                return;
            }

            // 5. Start deleting items with safety limit
            const MAX_ITERATIONS = 10000; // Safety limit to prevent infinite loops
            let currentTotal = originalTotal;
            const deletedItemsByBill = {};
            let iterations = 0;

            while (currentTotal > targetAmount && iterations < MAX_ITERATIONS) {
                let deleted = false;

                for (const bill of bills) {
                    if (bill.items && bill.items.length > 1) {
                        const item = bill.items[0]; // highest priced item
                        bill.items.shift(); // remove from array

                        // Track deleted items by bill
                        if (!deletedItemsByBill[bill.billId]) {
                            deletedItemsByBill[bill.billId] = {
                                billDetails: {
                                    billId: bill.billId,
                                    billNumber: bill.billNumber,
                                    billDate: bill.billDate,
                                    originalTotal: parseFloat(bill.totalAmount || 0)
                                },
                                items: []
                            };
                        }
                        deletedItemsByBill[bill.billId].items.push(item);

                        // Delete from DB
                        await queryAsync(
                            conn,
                            `DELETE FROM billing_billWiseItem_data WHERE iwbId = ?`,
                            [item.iwbId]
                        );

                        // Recalculate bill totals using helper function
                        const totals = calculateBillTotals(bill, bill.items);
                        bill.totalAmount = totals.totalAmount;
                        bill.totalDiscount = totals.totalDiscount;
                        bill.settledAmount = totals.settledAmount;

                        // Update bill in DB
                        await queryAsync(
                            conn,
                            `UPDATE billing_data 
                             SET totalAmount = ?, totalDiscount = ?, settledAmount = ? 
                             WHERE billId = ?`,
                            [bill.totalAmount, bill.totalDiscount, bill.settledAmount, bill.billId]
                        );

                        currentTotal -= parseFloat(item.price || 0);
                        deleted = true;
                        break; // only one item per iteration
                    }
                }

                if (!deleted) break; // no more items can be deleted
                iterations++;
            }

            if (iterations >= MAX_ITERATIONS) {
                console.warn(`Reached max iterations (${MAX_ITERATIONS}). Target may not be achievable.`);
            }

            // 6. Build summary statistics
            const allDeleted = Object.values(deletedItemsByBill).flatMap(b => b.items);
            let summary = {};
            if (allDeleted.length > 0) {
                const prices = allDeleted.map(d => parseFloat(d.price || 0)).filter(p => !isNaN(p));
                if (prices.length > 0) {
                    summary = {
                        iterations,
                        totalDeletedItems: allDeleted.length,
                        largestDeletedItem: Math.max(...prices),
                        smallestDeletedItem: Math.min(...prices),
                        averageDeletedValue: (prices.reduce((a, b) => a + b, 0) / prices.length).toFixed(2),
                        totalDeletedValue: prices.reduce((a, b) => a + b, 0).toFixed(2)
                    };
                } else {
                    summary = { iterations, totalDeletedItems: allDeleted.length };
                }
            } else {
                summary = { iterations, totalDeletedItems: 0 };
            }

            // 7. Commit changes
            await queryAsync(conn, "COMMIT");

            // 8. Return simple success response
            const amountReduction = originalTotal - currentTotal;
            res.status(200).json({
                message: `Bills settled successfully. ${summary.totalDeletedItems} items deleted, reducing total by ${amountReduction.toFixed(2)}`,
                statusCode: 200
            });

        } catch (error) {
            console.error("Error in settleBills:", error);
            try {
                await queryAsync(conn, "ROLLBACK");
            } catch (rollbackError) {
                console.error("Error rolling back transaction:", rollbackError);
            }
            res.status(500).json({
                message: "Internal server error",
                statusCode: 500
            });
        } finally {
            conn.release();
        }
    });
};

const getTempTestData = (req, res) => {
    try {
        let sql_query_getTempTestData = `SELECT startDate, endDate, creationDate FROM temp_test_data ORDER BY creationDate desc LIMIT 10`;
        pool.query(sql_query_getTempTestData, (err, data) => {
            if (err) {
                console.error("Error in getTempTestData:", err);
                res.status(500).send({ message: "Internal server error", error: err.message || "Unknown error" || error.message });
            }
            else {
                res.status(200).send(data || []);
            }
        });
    } catch (error) {
        console.error("Error in getTempTestData:", error);
        res.status(500).send({ message: "Internal server error", error: error.message || "Unknown error" });
    }
}

module.exports = {
    settleBills,
    dryRunSettleBills,
    getTempTestData
}