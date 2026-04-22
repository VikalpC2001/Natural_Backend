const pool = require('../../database');
const jwt = require("jsonwebtoken");
const pool2 = require('../../databasePool');
const { componentsToColor } = require('pdf-lib');

// Get Date Function 4 Hour

function getCurrentDate() {
    const now = new Date();
    const hours = now.getHours();

    if (hours <= 4) { // If it's 4 AM or later, increment the date
        now.setDate(now.getDate() - 1);
    }
    return now.toDateString().slice(4, 15);
}

/**
 * Stable signature of selected addon ids for diffing (object keyed by addOnsId).
 *
 * @param {Record<string, unknown> | null | undefined} addons
 * @returns {string}
 */
function getAddonIdsSignature(addons) {
    if (!addons || typeof addons !== 'object') {
        return '';
    }
    return Object.keys(addons).sort().join('|');
}

// Compare Two Json Function

function compareJson(json1, json2) {
    const json1Map = new Map(json1.map(item => [item.iwbId, item]));
    const json2Map = new Map(json2.map(item => [item.iwbId, item]));

    const added = json2.filter(item => !json1Map.has(item.iwbId));
    const removed = json1.filter(item => !json2Map.has(item.iwbId));

    const modified = json1
        .filter(item => json2Map.has(item.iwbId)) // Check if the item exists in both json1 and json2
        .filter(item => {
            const json2Item = json2Map.get(item.iwbId);

            // Compare qty, price, comment, unit, itemPrice, and addon selection
            return (
                item.qty !== json2Item.qty ||
                item.unit !== json2Item.unit ||
                item.itemPrice !== json2Item.itemPrice ||
                item.price !== json2Item.price ||
                ((item.comment ? item.comment : '') !== (json2Item.comment ? json2Item.comment : '')) ||
                getAddonIdsSignature(item.addons) !== getAddonIdsSignature(json2Item.addons)
            );
        })
        .map(item => ({
            old: item, // Return the entire old object
            new: json2Map.get(item.iwbId) // Return the entire new object
        }));

    return { added, removed, modified };
}

/**
 * Collapse bill-item rows (one per addon join) into one object per iwbId with an addons map.
 *
 * @param {Array<Record<string, unknown>>} data
 * @returns {Array<Record<string, unknown>>}
 */
function groupItemsWithAddons(data) {
    const grouped = {};

    data.forEach(row => {
        const id = row.iwbId;
        if (!id) {
            return;
        }

        if (!grouped[id]) {
            grouped[id] = {
                iwbId: row.iwbId,
                itemId: row.itemId,
                inputCode: row.inputCode,
                itemName: row.itemName,
                preferredName: row.preferredName,
                qty: row.qty,
                unit: row.unit,
                itemPrice: row.itemPrice,
                price: row.price,
                comment: row.comment,
                kotItemStatus: row.kotItemStatus,
                addons: {}
            };
        }

        if (row.addOnsId) {
            grouped[id].addons[row.addOnsId] = {
                iwaId: row.iwaId,
                iwbId: row.iwbId,
                addOnsId: row.addOnsId,
                addonsName: row.addonsName,
                addonsGujaratiName: row.addonsGujaratiName,
                addonPrice: row.addonPrice
            };
        }
    });

    return Object.values(grouped);
}

// Add Dine In Order By App

const addDineInOrderByApp = (req, res) => {
    pool2.getConnection((err, connection) => {
        if (err) {
            console.error("Error getting database connection:", err);
            return res.status(500).send('Database Error');
        }
        try {
            connection.beginTransaction((err) => {
                if (err) {
                    console.error("Error beginning transaction:", err);
                    connection.release();
                    return res.status(500).send('Database Error');
                } else {
                    let token;
                    token = req.headers ? req.headers.authorization.split(" ")[1] : null;
                    if (token) {
                        const decoded = jwt.verify(token, process.env.JWT_SECRET);
                        const cashier = decoded.id.firstName;
                        const branchId = decoded.id.branchId;

                        const currentDate = getCurrentDate();
                        const billData = req.body;

                        if (!branchId || !billData.areaId || !billData.tableNo || !billData.subTotal || !billData.settledAmount || !billData.itemsData.length) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(404).send('Please Fill All The Fields..!');
                            })
                        } else {
                            let sql_query_getAdminId = `SELECT adminMacAddress FROM billing_admin_data LIMIT 1`;
                            connection.query(sql_query_getAdminId, (err, macId) => {
                                if (err) {
                                    console.error("Error selecting admin mac address:", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else {
                                    const adminMacAddress = macId && macId.length ? macId[0].adminMacAddress : null;
                                    let sql_query_chkTableAcvtive = `SELECT billId FROM billing_DineInTable_data WHERE tableNo = '${billData.tableNo}' AND areaId = '${billData.areaId}'`;
                                    connection.query(sql_query_chkTableAcvtive, (err, table) => {
                                        if (err) {
                                            console.error("Error selecting last bill and token number:", err);
                                            connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send('Database Error');
                                            });
                                        } else {
                                            if (table && table.length && table[0].billId) {
                                                const existBillId = table[0].billId;
                                                let sql_query_getLastSubToken = `SELECT COALESCE(MAX(subTokenNumber),0) AS lastSubTokenNo FROM billing_subtoken_data WHERE branchId = '${branchId}' AND subTokenDate = STR_TO_DATE('${currentDate}','%b %d %Y') FOR UPDATE`;
                                                connection.query(sql_query_getLastSubToken, (err, result) => {
                                                    if (err) {
                                                        console.error("Error selecting last Sub Token number:", err);
                                                        connection.rollback(() => {
                                                            connection.release();
                                                            return res.status(500).send('Database Error');
                                                        });
                                                    } else {
                                                        const lastSubTokenNo = result && result[0] && result[0].lastSubTokenNo ? result[0].lastSubTokenNo : 0;

                                                        const nextSubTokenNo = lastSubTokenNo + 1;
                                                        const uid1 = new Date();
                                                        const subTokenId = String("subToken_" + uid1.getTime() + '_' + nextSubTokenNo);

                                                        let sql_querry_updateBillData = `UPDATE
                                                                                     billing_data
                                                                                 SET
                                                                                     totalAmount = totalAmount + ${billData.subTotal},
                                                                                     settledAmount = settledAmount + ${billData.settledAmount}
                                                                                 WHERE
                                                                                     billId = '${existBillId}';
                                                                                 UPDATE 
                                                                                    billing_billWiseTableNo_data 
                                                                                 SET 
                                                                                    assignCaptain = '${billData.assignCaptain ? billData.assignCaptain : cashier}' 
                                                                                 WHERE 
                                                                                    billId = '${existBillId}';`;
                                                        connection.query(sql_querry_updateBillData, (err) => {
                                                            if (err) {
                                                                console.error("Error Update new bill Data:", err);
                                                                connection.rollback(() => {
                                                                    connection.release();
                                                                    return res.status(500).send('Database Error');
                                                                });
                                                            } else {
                                                                let sql_query_addTokenNo = `INSERT INTO billing_subToken_data (subTokenId, captain, branchId, billId, subTokenNumber, tokenComment, subTokenDate, tokenStatus)
                                                                                            VALUES ('${subTokenId}', '${cashier}', '${branchId}', '${existBillId}', ${nextSubTokenNo}, ${billData.billComment ? `'${billData.billComment}'` : null}, STR_TO_DATE('${currentDate}','%b %d %Y'), 'print');`;
                                                                connection.query(sql_query_addTokenNo, (err) => {
                                                                    if (err) {
                                                                        console.error("Error inserting New Sub Token Number:", err);
                                                                        connection.rollback(() => {
                                                                            connection.release();
                                                                            return res.status(500).send('Database Error');
                                                                        });
                                                                    } else {
                                                                        const billItemData = billData.itemsData

                                                                        const addBillWiseItemData = [];
                                                                        const addItemWiseAddonData = [];
                                                                        let iwbIdArray = []

                                                                        billItemData.forEach((item, index) => {
                                                                            let uniqueId = `iwb_${Date.now() + index}_${index}`; // Unique ID generation
                                                                            iwbIdArray = [...iwbIdArray, uniqueId]
                                                                            // Construct SQL_Add_1 for the main item
                                                                            addBillWiseItemData.push(`('${uniqueId}', '${existBillId}', '${branchId}', '${item.itemId}', ${item.qty}, '${item.unit}', ${item.itemPrice}, ${item.price}, ${item.comment ? `'${item.comment}'` : null}, 'Dine In', 'cash', 'running', STR_TO_DATE('${currentDate}','%b %d %Y'))`);

                                                                            // Construct SQL_Add_2 for the addons
                                                                            const allAddons = item.addons ? Object.keys(item.addons) : []
                                                                            if (allAddons && allAddons.length) {
                                                                                allAddons.forEach((addonId, addonIndex) => {
                                                                                    let iwaId = `iwa_${Date.now() + addonIndex + index}_${index}`; // Unique ID for each addon
                                                                                    addItemWiseAddonData.push(`('${iwaId}', '${uniqueId}', '${addonId}')`);
                                                                                });
                                                                            }
                                                                        });

                                                                        let sql_query_addItems = `INSERT INTO billing_billWiseItem_data (iwbId, billId, branchId, itemId, qty, unit, itemPrice, price, comment, billType, billPayType, billStatus, billDate)
                                                                                          VALUES ${addBillWiseItemData}`;
                                                                        connection.query(sql_query_addItems, (err) => {
                                                                            if (err) {
                                                                                console.error("Error inserting Bill Wise Item Data:", err);
                                                                                connection.rollback(() => {
                                                                                    connection.release();
                                                                                    return res.status(500).send('Database Error');
                                                                                });
                                                                            } else {
                                                                                let addItemWiseSubToken = iwbIdArray.map((item, index) => {
                                                                                    let uniqueId = `iwst_${Date.now() + index + '_' + index}`;  // Generating a unique ID using current timestamp
                                                                                    return `('${uniqueId}', '${subTokenId}', '${item}')`;
                                                                                }).join(', ');
                                                                                let sql_query_addItems = `INSERT INTO billing_itemWiseSubToken_data (iwstId, subTokenId, iwbId)
                                                                                                  VALUES ${addItemWiseSubToken};
                                                                                                  ${addItemWiseAddonData.length ? `INSERT INTO billing_itemWiseAddon_data (iwaId, iwbId, addOnsId) VALUES ${addItemWiseAddonData.join(", ")}` : ''}`;
                                                                                connection.query(sql_query_addItems, (err) => {
                                                                                    if (err) {
                                                                                        console.error("Error inserting Item Wise Sub Token Id:", err);
                                                                                        connection.rollback(() => {
                                                                                            connection.release();
                                                                                            return res.status(500).send('Database Error');
                                                                                        });
                                                                                    } else {
                                                                                        connection.commit((err) => {
                                                                                            if (err) {
                                                                                                console.error("Error committing transaction:", err);
                                                                                                connection.rollback(() => {
                                                                                                    connection.release();
                                                                                                    return res.status(500).send('Database Error');
                                                                                                });
                                                                                            } else {
                                                                                                const sendJson = {
                                                                                                    ...billData,
                                                                                                    assignCaptain: billData.assignCaptain ? billData.assignCaptain : cashier,
                                                                                                    tokenNo: nextSubTokenNo ? nextSubTokenNo : 0,
                                                                                                    billDate: new Date(currentDate).toLocaleDateString('en-GB'),
                                                                                                    billTime: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
                                                                                                    isRunning: true
                                                                                                }
                                                                                                connection.release();
                                                                                                req?.io?.emit('updateTableView');
                                                                                                if (billData.isPrintKOT && adminMacAddress) {
                                                                                                    req?.io?.emit(`print_Kot_${adminMacAddress}`, sendJson);
                                                                                                }
                                                                                                return res.status(200).send(sendJson);
                                                                                            }
                                                                                        });
                                                                                    }
                                                                                })
                                                                            }
                                                                        })
                                                                    }
                                                                });
                                                            }
                                                        });
                                                    }
                                                });
                                            } else {
                                                let sql_query_chkTable = `SELECT tableNo FROM billing_DineInTable_data WHERE areaId = '${billData.areaId}' AND tableNo = '${billData.tableNo}' AND billId IS NOT NULL`;
                                                connection.query(sql_query_chkTable, (err, chkTable) => {
                                                    if (err) {
                                                        console.error("Error in Check Dine In Is Empty..!:", err);
                                                        connection.rollback(() => {
                                                            connection.release();
                                                            return res.status(500).send('Database Error');
                                                        });
                                                    } else {
                                                        if (chkTable && chkTable.length) {
                                                            connection.rollback(() => {
                                                                connection.release();
                                                                return res.status(401).send('Table Is Not Empty..!');
                                                            });
                                                        } else {
                                                            let sql_query_chkExistTable = `SELECT tableNo FROM billing_DineInTable_data WHERE areaId = '${billData.areaId}' AND tableNo = '${billData.tableNo}'`;
                                                            connection.query(sql_query_chkExistTable, (err, tbl) => {
                                                                if (err) {
                                                                    console.error("Error inserting new bill number:", err);
                                                                    connection.rollback(() => {
                                                                        connection.release();
                                                                        return res.status(500).send('Database Error');
                                                                    });
                                                                } else {
                                                                    let sql_query_getTableNo = tbl && tbl.length
                                                                        ?
                                                                        `SELECT tableNo FROM billing_DineInTable_data WHERE areaId = '${billData.areaId}' AND tableNo = '${billData.tableNo}'`
                                                                        :
                                                                        `INSERT INTO billing_DineInTable_data(tableId, areaId, tableNo, billId, isFixed)
                                                                 VALUES ('${billData.tableNo}', '${billData.areaId}', '${billData.tableNo}', NULL, 0)`;
                                                                    connection.query(sql_query_getTableNo, (err) => {
                                                                        if (err) {
                                                                            console.error("Error inserting new Table No:", err);
                                                                            connection.rollback(() => {
                                                                                connection.release();
                                                                                return res.status(500).send('Database Error');
                                                                            });
                                                                        } else {
                                                                            let sql_query_getLastBillNo = `SELECT COALESCE(MAX(billNumber),0) AS lastBillNo FROM billing_data WHERE branchId = '${branchId}' AND firmId = '${billData.firmId}' AND billCreationDate = (SELECT MAX(billCreationDate) FROM billing_data WHERE firmId = '${billData.firmId}' AND branchId = '${branchId}') FOR UPDATE;
                                                                                                   SELECT COALESCE(MAX(tokenNo),0) AS lastTokenNo FROM billing_token_data WHERE billType = 'Dine In' AND branchId = '${branchId}' AND billDate = STR_TO_DATE('${currentDate}','%b %d %Y') FOR UPDATE;
                                                                                                   SELECT COALESCE(MAX(subTokenNumber),0) AS lastSubTokenNo FROM billing_subtoken_data WHERE branchId = '${branchId}' AND subTokenDate = STR_TO_DATE('${currentDate}','%b %d %Y') FOR UPDATE;
                                                                                                   SELECT firmId FROM billing_branchWiseCategory_data WHERE branchId = '${branchId}' AND categoryId = 'dineIn';`;
                                                                            connection.query(sql_query_getLastBillNo, (err, result) => {
                                                                                if (err) {
                                                                                    console.error("Error selecting last bill and token number:", err);
                                                                                    connection.rollback(() => {
                                                                                        connection.release();
                                                                                        return res.status(500).send('Database Error');
                                                                                    });
                                                                                } else {
                                                                                    const lastBillNo = result && result[0] && result[0][0].lastBillNo ? result[0][0].lastBillNo : 0;
                                                                                    const lastTokenNo = result && result[1] && result[1][0].lastTokenNo ? result[1][0].lastTokenNo : 0;
                                                                                    const lastSubTokenNo = result && result[2] && result[2][0].lastSubTokenNo ? result[2][0].lastSubTokenNo : 0;
                                                                                    const firmId = result && result[3] && result[3][0].firmId ? result[3][0].firmId : 'C';

                                                                                    const nextBillNo = lastBillNo + 1;
                                                                                    const nextTokenNo = lastTokenNo + 1;
                                                                                    const nextSubTokenNo = lastSubTokenNo + 1;
                                                                                    const uid1 = new Date();
                                                                                    const billId = String("bill_" + uid1.getTime() + '_' + nextBillNo);
                                                                                    const tokenId = String("token_" + uid1.getTime() + '_' + nextTokenNo);
                                                                                    const subTokenId = String("subToken_" + uid1.getTime() + '_' + nextSubTokenNo);
                                                                                    const bwtId = String("bwtId_" + uid1.getTime() + '_' + nextBillNo);

                                                                                    const columnData = `billId,
                                                                                                firmId,
                                                                                                branchId,
                                                                                                cashier,
                                                                                                menuStatus,
                                                                                                billType,
                                                                                                billPayType,
                                                                                                discountType,
                                                                                                discountValue,
                                                                                                totalDiscount,
                                                                                                totalAmount,
                                                                                                settledAmount,
                                                                                                billComment,
                                                                                                billDate,
                                                                                                billStatus`;
                                                                                    const values = `'${billId}',
                                                                                            '${firmId}',
                                                                                            '${branchId}', 
                                                                                            '${cashier}', 
                                                                                            'Offline',
                                                                                            'Dine In',
                                                                                            'cash',
                                                                                            'none',
                                                                                            0,
                                                                                            0,
                                                                                            ${billData.subTotal},
                                                                                            ${billData.settledAmount},
                                                                                            NULL,
                                                                                            STR_TO_DATE('${currentDate}','%b %d %Y'),
                                                                                            'running'`;
                                                                                    let sql_querry_addBillData = `INSERT INTO billing_data (billNumber, ${columnData}) VALUES (${nextBillNo}, ${values});
                                                                                                          UPDATE billing_DineInTable_data SET billId = '${billId}' WHERE areaId = '${billData.areaId}' AND tableNo = '${billData.tableNo}'`;
                                                                                    connection.query(sql_querry_addBillData, (err) => {
                                                                                        if (err) {
                                                                                            console.error("Error inserting new bill number:", err);
                                                                                            connection.rollback(() => {
                                                                                                connection.release();
                                                                                                return res.status(500).send('Database Error');
                                                                                            });
                                                                                        } else {
                                                                                            let sql_query_addTokenNo = `INSERT INTO billing_token_data(tokenId, billId, branchId, tokenNo, billType, billDate)
                                                                                                                VALUES ('${tokenId}', '${billId}', '${branchId}', ${nextTokenNo}, 'Dine In', STR_TO_DATE('${currentDate}','%b %d %Y'));
                                                                                                                INSERT INTO billing_subToken_data(subTokenId, captain, branchId, billId, subTokenNumber, tokenComment, subTokenDate, tokenStatus)
                                                                                                                VALUES ('${subTokenId}', '${cashier}', '${branchId}', '${billId}', ${nextSubTokenNo}, ${billData.billComment ? `'${billData.billComment}'` : null}, STR_TO_DATE('${currentDate}','%b %d %Y'), 'print');`;
                                                                                            connection.query(sql_query_addTokenNo, (err) => {
                                                                                                if (err) {
                                                                                                    console.error("Error inserting new Token & Sub Token number:", err);
                                                                                                    connection.rollback(() => {
                                                                                                        connection.release();
                                                                                                        return res.status(500).send('Database Error');
                                                                                                    });
                                                                                                } else {
                                                                                                    let sql_query_addBillWiseTable = `INSERT INTO billing_billWiseTableNo_data(bwtId, billId, areaId, tableNo, assignCaptain, printTime)
                                                                                                                              VALUES('${bwtId}', '${billId}', '${billData.areaId}', '${billData.tableNo}', '${billData.assignCaptain ? billData.assignCaptain : cashier}', NOW())`;
                                                                                                    connection.query(sql_query_addBillWiseTable, (err) => {
                                                                                                        if (err) {
                                                                                                            console.error("Error inserting Bill Wise Table Data:", err);
                                                                                                            connection.rollback(() => {
                                                                                                                connection.release();
                                                                                                                return res.status(500).send('Database Error');
                                                                                                            });
                                                                                                        } else {
                                                                                                            const billItemData = billData.itemsData

                                                                                                            const addBillWiseItemData = [];
                                                                                                            const addItemWiseAddonData = [];
                                                                                                            let iwbIdArray = []

                                                                                                            billItemData.forEach((item, index) => {
                                                                                                                let uniqueId = `iwb_${Date.now() + index}_${index}`; // Unique ID generation
                                                                                                                iwbIdArray = [...iwbIdArray, uniqueId]

                                                                                                                // Construct SQL_Add_1 for the main item
                                                                                                                addBillWiseItemData.push(`('${uniqueId}', '${billId}', '${branchId}', '${item.itemId}', ${item.qty}, '${item.unit}', ${item.itemPrice}, ${item.price}, ${item.comment ? `'${item.comment}'` : null}, 'Dine In', 'cash', 'print', STR_TO_DATE('${currentDate}','%b %d %Y'))`);

                                                                                                                // Construct SQL_Add_2 for the addons
                                                                                                                const allAddons = item.addons ? Object.keys(item.addons) : []
                                                                                                                if (allAddons && allAddons.length) {
                                                                                                                    allAddons.forEach((addonId, addonIndex) => {
                                                                                                                        let iwaId = `iwa_${Date.now() + addonIndex + index}_${index}`; // Unique ID for each addon
                                                                                                                        addItemWiseAddonData.push(`('${iwaId}', '${uniqueId}', '${addonId}')`);
                                                                                                                    });
                                                                                                                }
                                                                                                            });
                                                                                                            let sql_query_addItems = `INSERT INTO billing_billWiseItem_data(iwbId, billId, branchId, itemId, qty, unit, itemPrice, price, comment, billType, billPayType, billStatus, billDate)
                                                                                                                              VALUES ${addBillWiseItemData}`;
                                                                                                            connection.query(sql_query_addItems, (err) => {
                                                                                                                if (err) {
                                                                                                                    console.error("Error inserting Bill Wise Item Data:", err);
                                                                                                                    connection.rollback(() => {
                                                                                                                        connection.release();
                                                                                                                        return res.status(500).send('Database Error');
                                                                                                                    });
                                                                                                                } else {
                                                                                                                    let addItemWiseSubToken = iwbIdArray.map((item, index) => {
                                                                                                                        let uniqueId = `iwst_${Date.now() + index + '_' + index}`;  // Generating a unique ID using current timestamp
                                                                                                                        return `('${uniqueId}', '${subTokenId}', '${item}')`;
                                                                                                                    }).join(', ');
                                                                                                                    let sql_query_addItems = `INSERT INTO billing_itemWiseSubToken_data(iwstId, subTokenId, iwbId)
                                                                                                                                      VALUES ${addItemWiseSubToken};
                                                                                                                                      ${addItemWiseAddonData.length ? `INSERT INTO billing_itemWiseAddon_data (iwaId, iwbId, addOnsId) VALUES ${addItemWiseAddonData.join(", ")}` : ''}`;
                                                                                                                    connection.query(sql_query_addItems, (err) => {
                                                                                                                        if (err) {
                                                                                                                            console.error("Error inserting Item Wise Sub Token Id:", err);
                                                                                                                            connection.rollback(() => {
                                                                                                                                connection.release();
                                                                                                                                return res.status(500).send('Database Error');
                                                                                                                            });
                                                                                                                        } else {
                                                                                                                            connection.commit((err) => {
                                                                                                                                if (err) {
                                                                                                                                    console.error("Error committing transaction:", err);
                                                                                                                                    connection.rollback(() => {
                                                                                                                                        connection.release();
                                                                                                                                        return res.status(500).send('Database Error');
                                                                                                                                    });
                                                                                                                                } else {
                                                                                                                                    const sendJson = {
                                                                                                                                        ...billData,
                                                                                                                                        assignCaptain: billData.assignCaptain ? billData.assignCaptain : cashier,
                                                                                                                                        tokenNo: nextSubTokenNo ? nextSubTokenNo : 0,
                                                                                                                                        billDate: new Date(currentDate).toLocaleDateString('en-GB'),
                                                                                                                                        billTime: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
                                                                                                                                    }
                                                                                                                                    connection.release();
                                                                                                                                    req?.io?.emit('updateTableView');
                                                                                                                                    if (billData.isPrintKOT && adminMacAddress) {
                                                                                                                                        req?.io?.emit(`print_Kot_${adminMacAddress}`, sendJson);
                                                                                                                                    }
                                                                                                                                    return res.status(200).send(sendJson);
                                                                                                                                }
                                                                                                                            });
                                                                                                                        }
                                                                                                                    })
                                                                                                                }
                                                                                                            })
                                                                                                        }
                                                                                                    });
                                                                                                }
                                                                                            });
                                                                                        }
                                                                                    });
                                                                                }
                                                                            });
                                                                        }
                                                                    })
                                                                }
                                                            })
                                                        }
                                                    }
                                                });
                                            }
                                        }
                                    })
                                }
                            });
                        }
                    } else {
                        connection.rollback(() => {
                            connection.release();
                            return res.status(404).send('Please Login First....!');
                        });
                    }
                }
            });
        } catch (error) {
            console.error('An error occurred', error);
            connection.rollback(() => {
                connection.release();
                return res.status(500).json('Internal Server Error');
            })
        }
    });
}

// Get Subtokens Details By Id

const getSubTokensByBillIdForApp = async (req, res) => {
    try {
        const billId = req.query.billId;
        if (!billId) {
            return res.status(401).send('Bill Id Not Found...!');
        } else {
            let sql_queries_getDetails = `SELECT 
                                              bst.subTokenId,
                                              bst.tokenComment,
                                              bst.captain,
                                              bwtn.assignCaptain,
                                              DATE_FORMAT(bst.subTokenDate, '%d/%m/%Y') AS subTokenDate,
                                              DATE_FORMAT(bst.creationDate, '%h:%i %p') AS createTime,
                                              bst.subTokenNumber,
                                              bst.tokenStatus,
                                              iwst.iwbId,
                                              iwst.itemStatus AS kotItemStatus,
                                              COALESCE(bwi.itemId, bmk.itemId) AS itemId,
                                              imld.itemName AS itemName,
                                              uwp.preferredName AS preferredName,
                                              imld.itemCode AS inputCode,
                                              COALESCE(bwi.qty, bmk.qty) AS qty,
                                              COALESCE(bwi.unit, bmk.unit) AS unit,
                                              COALESCE(bwi.itemPrice, bmk.itemPrice) AS itemPrice,
                                              COALESCE(bwi.price, bmk.price) AS price,
                                              COALESCE(bwi.comment, bmk.comment) AS comment,
                                              iwad.iwaId,
                                              iwad.addOnsId,
                                              iad.addonsName,
                                              iad.addonsGujaratiName,
                                              iad.price AS addonPrice
                                          FROM billing_subToken_data bst
                                          LEFT JOIN billing_itemWiseSubToken_data iwst ON iwst.subTokenId = bst.subTokenId    
                                          LEFT JOIN billing_billWiseItem_data bwi ON bwi.iwbId = iwst.iwbId
                                          LEFT JOIN billing_modifiedKot_data bmk ON bmk.iwbId = iwst.iwbId
                                          LEFT JOIN item_menuList_data imld ON imld.itemId = COALESCE(bwi.itemId, bmk.itemId)
                                          LEFT JOIN item_unitWisePrice_data uwp ON uwp.itemId = COALESCE(bwi.itemId, bmk.itemId) AND uwp.unit = COALESCE(bwi.unit, bmk.unit) AND uwp.menuCategoryId = '${process.env.BASE_MENU}'
                                          LEFT JOIN billing_billWiseTableNo_data bwtn ON bwtn.billId = bst.billId
                                          LEFT JOIN billing_itemWiseAddon_data iwad ON iwad.iwbId = iwst.iwbId
                                          LEFT JOIN item_addons_data iad ON iad.addonsId = iwad.addOnsId
                                          WHERE bst.billId = '${billId}' AND bst.tokenStatus != 'cancelled'
                                          ORDER BY bst.creationDate DESC, bst.subTokenNumber DESC;
                                          SELECT
                                             bwid.iwbId AS iwbId,
                                             bwid.itemId AS itemId,
                                             imd.itemName AS itemName,
                                             imd.itemCode AS inputCode,
                                             SUM(bwid.qty) AS qty,
                                             bwid.unit AS unit,
                                             bwid.itemPrice AS itemPrice,
                                             SUM(bwid.price) AS price
                                         FROM
                                             billing_billWiseItem_data AS bwid
                                         INNER JOIN item_menuList_data AS imd ON imd.itemId = bwid.itemId
                                         WHERE bwid.billId = '${billId}'
                                         GROUP BY bwid.itemId, bwid.unit`;
            pool.query(sql_queries_getDetails, (err, data) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    const subTokensJson = data && data[0].length ? data[0] : [];
                    const mergedItemJson = data && data[1].length ? data[1] : [];

                    const result = subTokensJson.reduce((acc, row) => {

                        let token = acc.find(t => t.subTokenId === row.subTokenId);

                        const shouldIncludeInTotal = row.kotItemStatus !== 'cancelled';

                        if (!token) {
                            // Create new token group
                            token = {
                                subTokenId: row.subTokenId,
                                assignCaptain: row.assignCaptain,
                                captain: row.captain,
                                subTokenNumber: row.subTokenNumber,
                                tokenStatus: row.tokenStatus,
                                subTokenDate: row.subTokenDate,
                                createTime: row.createTime,
                                tokenComment: row.tokenComment,
                                totalPrice: shouldIncludeInTotal ? row.price : 0,
                                items: []
                            };
                            acc.push(token);
                        } else {
                            // Update total price
                            if (shouldIncludeInTotal) token.totalPrice += row.price;
                        }

                        if (!shouldIncludeInTotal) {
                            return acc;
                        }

                        // Find existing item by iwbId
                        let item = token.items.find(it => it.iwbId === row.iwbId);

                        if (!item) {
                            // Create new item
                            item = {
                                iwbId: row.iwbId,
                                itemId: row.itemId,
                                inputCode: row.inputCode,
                                itemName: row.itemName,
                                preferredName: row.preferredName,
                                qty: row.qty,
                                unit: row.unit,
                                itemPrice: row.itemPrice,
                                price: row.price,
                                comment: row.comment,
                                kotItemStatus: row.kotItemStatus,
                                addons: {}  // <-- ADDONS OBJECT
                            };
                            token.items.push(item);
                        }

                        // Add addon only when exists
                        if (row.addOnsId) {
                            item.addons[row.addOnsId] = {
                                iwaId: row.iwaId,
                                iwbId: row.iwbId,
                                addOnsId: row.addOnsId,
                                addonsName: row.addonsName,
                                addonsGujaratiName: row.addonsGujaratiName,
                                addonPrice: row.addonPrice
                            };
                        }

                        return acc;

                    }, []);

                    const subTokens = result.map(token => {
                        const filteredItems = token.items.filter(item => {
                            const primaryFields = ['iwbId', 'itemId', 'itemName', 'qty', 'price'];
                            return primaryFields.some(field => {
                                const v = item[field];
                                // treat non-empty strings, numbers, and non-empty objects as valid
                                if (v === null || v === undefined) return false;
                                if (typeof v === 'string') return v.trim() !== '';
                                if (typeof v === 'object') return Object.keys(v).length > 0;
                                return true; // number, boolean, etc.
                            });
                        });
                        return {
                            ...token,
                            items: filteredItems.length ? filteredItems : [{ itemName: "Item deleted from bill.", unit: "", qty: "" }]
                        };
                    });
                    return res.status(200).send({ subTokens, mergedItemJson });
                }
            });
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Get Subtokens Details By Id

const getSubTokenDataByIdForApp = async (req, res) => {
    try {
        const billId = req.query.billId;
        const subTokenId = req.query.subTokenId;
        if (!billId || !subTokenId) {
            return res.status(401).send('BillId or SubTokenId Not Found...!');
        } else {
            let sql_queries_getDetails = `SELECT
                                              bst.subTokenId AS subTokenId,
                                              bst.tokenComment AS tokenComment,
                                              bwtn.assignCaptain AS captain,
                                              DATE_FORMAT(bst.subTokenDate, '%d/%m/%Y') AS subTokenDate,
                                              DATE_FORMAT(bst.creationDate, '%h:%i %p') AS createTime,
                                              bst.subTokenNumber AS subTokenNumber,
                                              bst.tokenStatus AS tokenStatus,
                                              iwst.iwbId AS iwbId,
                                              iwst.itemStatus AS kotItemStatus,
                                              COALESCE(bwi.itemId, bmk.itemId) AS itemId,
                                              imld.itemName AS itemName,
                                              uwp.preferredName AS preferredName,
                                              imld.itemCode AS inputCode,
                                              COALESCE(bwi.qty, bmk.qty) AS qty,
                                              COALESCE(bwi.unit, bmk.unit) AS unit,
                                              COALESCE(bwi.itemPrice, bmk.itemPrice) AS itemPrice,
                                              COALESCE(bwi.price, bmk.price) AS price,
                                              COALESCE(bwi.comment, bmk.comment) AS comment,
                                              iwad.iwaId,
                                              iwad.addOnsId,
                                              iad.addonsName,
                                              iad.addonsGujaratiName,
                                              iad.price AS addonPrice
                                          FROM
                                              billing_subToken_data AS bst
                                          LEFT JOIN billing_itemWiseSubToken_data AS iwst ON iwst.subTokenId = bst.subTokenId
                                          LEFT JOIN billing_billWiseItem_data AS bwi ON bwi.iwbId = iwst.iwbId
                                          LEFT JOIN billing_modifiedKot_data AS bmk ON bmk.iwbId = iwst.iwbId
                                          LEFT JOIN item_menuList_data AS imld ON imld.itemId = COALESCE(bwi.itemId, bmk.itemId)
                                          LEFT JOIN item_unitWisePrice_data AS uwp ON uwp.itemId = COALESCE(bwi.itemId, bmk.itemId) AND uwp.unit = COALESCE(bwi.unit, bmk.unit) AND uwp.menuCategoryId = '${process.env.BASE_MENU}'
                                          LEFT JOIN billing_billWiseTableNo_data AS bwtn ON bwtn.billId = bst.billId
                                          LEFT JOIN billing_itemWiseAddon_data AS iwad ON iwad.iwbId = iwst.iwbId
                                          LEFT JOIN item_addons_data AS iad ON iad.addonsId = iwad.addOnsId
                                          WHERE bst.subTokenId = '${subTokenId}' AND bst.billId = '${billId}' AND bst.tokenStatus != 'cancelled'
                                          ORDER BY bst.subTokenNumber DESC`;
            pool.query(sql_queries_getDetails, (err, data) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {

                    const result = data.reduce((acc, row) => {
                        let existingToken = acc.find(group => group.subTokenId === row.subTokenId);
                        const shouldIncludeInTotal = row.kotItemStatus !== 'cancelled';

                        if (!existingToken) {
                            existingToken = {
                                subTokenId: row.subTokenId,
                                captain: row.captain,
                                subTokenNumber: row.subTokenNumber,
                                tokenStatus: row.tokenStatus,
                                subTokenDate: row.subTokenDate,
                                createTime: row.createTime,
                                tokenComment: row.tokenComment,
                                totalPrice: 0,
                                totalQty: 0,
                                items: []
                            };
                            acc.push(existingToken);
                        }

                        let lineItem = existingToken.items.find(it => it.iwbId === row.iwbId);

                        if (!lineItem && shouldIncludeInTotal) {
                            lineItem = {
                                iwbId: row.iwbId,
                                itemId: row.itemId,
                                inputCode: row.inputCode,
                                itemName: row.itemName,
                                preferredName: row.preferredName,
                                qty: row.qty,
                                unit: row.unit,
                                itemPrice: row.itemPrice,
                                price: row.price,
                                comment: row.comment,
                                kotItemStatus: row.kotItemStatus,
                                addons: {}
                            };
                            existingToken.items.push(lineItem);
                            existingToken.totalPrice += row.price;
                            existingToken.totalQty += row.qty;
                        }

                        if (lineItem && row.addOnsId) {
                            lineItem.addons[row.addOnsId] = {
                                iwaId: row.iwaId,
                                iwbId: row.iwbId,
                                addOnsId: row.addOnsId,
                                addonsName: row.addonsName,
                                addonsGujaratiName: row.addonsGujaratiName,
                                addonPrice: row.addonPrice
                            };
                        }

                        return acc;
                    }, []);
                    return res.status(200).send(result[0]);
                }
            });
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Update Sub Token Data By APP

const updateSubTokenDataByIdForApp = (req, res) => {
    pool2.getConnection((err, connection) => {
        if (err) {
            console.error("Error getting database connection:", err);
            return res.status(500).send('Database Error');
        }
        try {
            connection.beginTransaction((err) => {
                if (err) {
                    console.error("Error beginning transaction:", err);
                    connection.release();
                    return res.status(500).send('Database Error');
                } else {
                    let token;
                    token = req.headers ? req.headers.authorization.split(" ")[1] : null;
                    if (token) {
                        const decoded = jwt.verify(token, process.env.JWT_SECRET);
                        const cashier = decoded.id.firstName;
                        const branchId = decoded.id.branchId;
                        const currentDate = getCurrentDate();
                        const billData = req.body;

                        if (!billData.subTokenId || !billData.billId || !billData.subTokenNumber || !billData.settledAmount || !billData.subTotal || !billData.itemsData.length) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(404).send('Please Fill All The Fields..!');
                            })
                        } else {
                            let sql_query_getAdminId = `SELECT adminMacAddress FROM billing_admin_data`;
                            connection.query(sql_query_getAdminId, (err, macId) => {
                                if (err) {
                                    console.error("Error Get Pre Total Price:", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else {
                                    if (macId && macId.length) {
                                        const adminMacAddress = macId[0].adminMacAddress;
                                        let sql_query_getPreTokenPrice = `SELECT SUM(bwi.price) AS preTotalPrice FROM billing_itemWiseSubToken_data  AS iwst
                                                                          LEFT JOIN billing_billWiseItem_data AS bwi ON bwi.iwbId = iwst.iwbId
                                                                          WHERE subTokenId = '${billData.subTokenId}'`;
                                        connection.query(sql_query_getPreTokenPrice, (err, prePrice) => {
                                            if (err) {
                                                console.error("Error Get Pre Total Price:", err);
                                                connection.rollback(() => {
                                                    connection.release();
                                                    return res.status(500).send('Database Error');
                                                });
                                            } else {
                                                const preTotalPrice = prePrice && prePrice[0].preTotalPrice ? prePrice[0].preTotalPrice : 0;
                                                let sql_querry_updateBillData = `UPDATE
                                                                                     billing_data
                                                                                 SET
                                                                                     totalAmount = totalAmount - ${preTotalPrice} + ${billData.subTotal},
                                                                                     settledAmount = settledAmount - ${preTotalPrice} + ${billData.settledAmount}
                                                                                 WHERE
                                                                                     billId = '${billData.billId}';
                                                                                 UPDATE
                                                                                     billing_subToken_data
                                                                                 SET
                                                                                     tokenComment = ${billData.billComment ? `'${billData.billComment}'` : null}
                                                                                 WHERE 
                                                                                    subTokenId = '${billData.subTokenId}';
                                                                                 UPDATE
                                                                                    billing_billWiseTableNo_data
                                                                                 SET
                                                                                    assignCaptain = '${billData.assignCaptain ? billData.assignCaptain : cashier}' 
                                                                                 WHERE 
                                                                                    billId = '${billData.billId}';`;
                                                connection.query(sql_querry_updateBillData, (err) => {
                                                    if (err) {
                                                        console.error("Error Update new bill Price:", err);
                                                        connection.rollback(() => {
                                                            connection.release();
                                                            return res.status(500).send('Database Error');
                                                        });
                                                    } else {
                                                        let sql_query_getOldItemJson = `SELECT
                                                                                            bwid.iwbId AS iwbId,
                                                                                            bwid.itemId AS itemId,
                                                                                            imd.itemCode AS inputCode,
                                                                                            imd.itemName AS itemName,
                                                                                            uwp.preferredName AS preferredName,
                                                                                            bwid.qty AS qty,
                                                                                            bwid.unit AS unit,
                                                                                            bwid.itemPrice AS itemPrice,
                                                                                            bwid.price AS price,
                                                                                            bwid.comment AS comment,
                                                                                            iwad.iwaId,
                                                                                            iwad.addOnsId,
                                                                                            iad.addonsName,
                                                                                            iad.addonsGujaratiName,
                                                                                            iad.price AS addonPrice
                                                                                        FROM
                                                                                            billing_billWiseItem_data AS bwid
                                                                                        INNER JOIN item_menuList_data AS imd ON imd.itemId = bwid.itemId
                                                                                        LEFT JOIN billing_itemWiseAddon_data iwad ON iwad.iwbId = bwid.iwbId
                                                                                        LEFT JOIN item_addons_data iad ON iad.addonsId = iwad.addOnsId
                                                                                        LEFT JOIN item_unitWisePrice_data AS uwp ON uwp.itemId = bwid.itemId AND uwp.unit = bwid.unit AND uwp.menuCategoryId = '${process.env.BASE_MENU}'
                                                                                        WHERE bwid.iwbId IN (SELECT COALESCE(iwbId,NULL) FROM billing_itemWiseSubToken_data WHERE subTokenId = '${billData.subTokenId}')`;
                                                        connection.query(sql_query_getOldItemJson, (err, oldJson) => {
                                                            if (err) {
                                                                console.error("Error getting old item json:", err);
                                                                connection.rollback(() => {
                                                                    connection.release();
                                                                    return res.status(500).send('Database Error');
                                                                });
                                                            } else {
                                                                const json1 = groupItemsWithAddons(JSON.parse(JSON.stringify(oldJson)));
                                                                const json2 = Object.values(JSON.parse(JSON.stringify(billData.itemsData)));

                                                                console.log('jason1', json1);
                                                                console.log('jason2', json2);

                                                                const { added, removed, modified } = compareJson(json1, json2);

                                                                console.log("addd+++", added);
                                                                console.log("Remove---", removed);
                                                                console.log("Updated", modified);

                                                                if (added.length || removed.length || modified.length) {
                                                                    const modifiedNewJson = modified.map(({ new: newItem }) => newItem);
                                                                    let addBillWiseItemData = [];
                                                                    let addItemWiseAddonData = [];
                                                                    let iwbIdAddArray = [];

                                                                    let addModifiedItemWiseAddonData = [];
                                                                    modifiedNewJson.forEach((item, index) => {
                                                                        const modAddonKeys = item.addons ? Object.keys(item.addons) : [];
                                                                        modAddonKeys.forEach((addonId, addonIndex) => {
                                                                            const iwaId = `iwa_m_${Date.now()}_${addonIndex}_${index}`;
                                                                            addModifiedItemWiseAddonData.push(`('${iwaId}', '${item.iwbId}', '${addonId}')`);
                                                                        });
                                                                    });

                                                                    // ADD New Item In Bill
                                                                    added.forEach((item, index) => {
                                                                        let uniqueId = `iwb_${Date.now() + index}_${index}`; // Unique ID generation
                                                                        iwbIdAddArray = [...iwbIdAddArray, uniqueId]
                                                                        // Construct SQL_Add_1 for the main item
                                                                        addBillWiseItemData.push(`('${uniqueId}', '${billData.billId}', '${branchId}', '${item.itemId}', ${item.qty}, '${item.unit}', ${item.itemPrice}, ${item.price}, ${item.comment ? `'${item.comment}'` : null}, 'Dine In', 'cash', 'running', STR_TO_DATE('${currentDate}','%b %d %Y'))`);

                                                                        // Construct SQL_Add_2 for the addons
                                                                        const allAddons = item.addons ? Object.keys(item.addons) : []
                                                                        if (allAddons && allAddons.length) {
                                                                            allAddons.forEach((addonId, addonIndex) => {
                                                                                let iwaId = `iwa_${Date.now() + addonIndex + index}_${index}`; // Unique ID for each addon
                                                                                addItemWiseAddonData.push(`('${iwaId}', '${uniqueId}', '${addonId}')`);
                                                                            });
                                                                        }
                                                                    });

                                                                    let addRemovedKotItem = removed.length ? removed.map((item, index) => {
                                                                        let uniqueId = `modified_${Date.now() + index + '_' + index}`; // Generating a unique ID using current timestamp
                                                                        return `('${uniqueId}', '${cashier}', '${item.iwbId}', '${item.itemId}', ${item.qty}, '${item.unit}', ${item.itemPrice}, ${item.price}, ${item.comment ? `'${item.comment}'` : null})`;
                                                                    }).join(', ') : '';

                                                                    // Remove Items iwbIds
                                                                    let removeJsonIds = removed.length ? removed.map((item, index) => {
                                                                        return `'${item.iwbId}'`;
                                                                    }).join(',') : '';

                                                                    // Updated Items iwbIds
                                                                    let updateJsonIds = modifiedNewJson.length ? modifiedNewJson.map((item, index) => {
                                                                        return `'${item.iwbId}'`;
                                                                    }).join(',') : '';

                                                                    // Update Existing Data Query

                                                                    let updateQuery = modifiedNewJson.length ?
                                                                        `UPDATE billing_billWiseItem_data SET qty = CASE iwbId ` +
                                                                        modifiedNewJson.map(item => `WHEN '${item.iwbId}' THEN ${item.qty}`).join(' ') +
                                                                        ` END,
                                                                         itemPrice = CASE iwbId ` +
                                                                        modifiedNewJson.map(item => `WHEN '${item.iwbId}' THEN ${item.itemPrice}`).join(' ') +
                                                                        ` END,
                                                                         price = CASE iwbId ` +
                                                                        modifiedNewJson.map(item => `WHEN '${item.iwbId}' THEN ${item.price}`).join(' ') +
                                                                        ` END,
                                                                         comment = CASE iwbId ` +
                                                                        modifiedNewJson.map(item => `WHEN '${item.iwbId}' THEN ${item.comment ? `'${item.comment}'` : null}`).join(' ') +
                                                                        ` END
                                                                         WHERE iwbId IN (${modifiedNewJson.map(item => `'${item.iwbId}'`).join(', ')});`
                                                                        : `SELECT * FROM user_details WHERE userId = '0';`;

                                                                    let sql_query_adjustItem = `${added.length ? `INSERT INTO billing_billWiseItem_data(iwbId, billId, branchId, itemId, qty, unit, itemPrice, price, comment, billType, billPayType, billStatus, billDate)
                                                                                                                  VALUES ${addBillWiseItemData};` : ''}
                                                                                                ${removed.length ? `INSERT INTO billing_modifiedAddons_data(madId, iwbId, addOnsId)
                                                                                                SELECT CONCAT('mad_', iwaId), iwbId, addOnsId FROM billing_itemWiseAddon_data WHERE iwbId IN (${removeJsonIds});
                                                                                                DELETE FROM billing_itemWiseAddon_data WHERE iwbId IN (${removeJsonIds});
                                                                                                DELETE FROM billing_billWiseItem_data WHERE iwbId IN (${removeJsonIds});` : ''}
                                                                                                ${updateQuery}
                                                                                                ${modifiedNewJson.length ? `INSERT INTO billing_modifiedAddons_data(madId, iwbId, addOnsId)
                                                                                                SELECT CONCAT('mad_', iwaId), iwbId, addOnsId FROM billing_itemWiseAddon_data WHERE iwbId IN (${updateJsonIds});
                                                                                                DELETE FROM billing_itemWiseAddon_data WHERE iwbId IN (${updateJsonIds});` : ''}
                                                                                                ${addModifiedItemWiseAddonData.length ? `INSERT INTO billing_itemWiseAddon_data (iwaId, iwbId, addOnsId) VALUES ${addModifiedItemWiseAddonData.join(', ')};` : ''}`;
                                                                    connection.query(sql_query_adjustItem, (err) => {
                                                                        if (err) {
                                                                            console.error("Error inserting Bill Wise Item Data:", err);
                                                                            connection.rollback(() => {
                                                                                connection.release();
                                                                                return res.status(500).send('Database Error');
                                                                            });
                                                                        } else {
                                                                            let addItemWiseSubToken = iwbIdAddArray.length ? iwbIdAddArray.map((item, index) => {
                                                                                let uniqueId = `iwst_${Date.now() + index + '_' + index}`;  // Generating a unique ID using current timestamp
                                                                                return `('${uniqueId}', '${billData.subTokenId}', '${item}', 'new')`;
                                                                            }).join(', ') : '';
                                                                            let sql_query_addItemsId = iwbIdAddArray.length
                                                                                ?
                                                                                `INSERT INTO billing_itemWiseSubToken_data(iwstId, subTokenId, iwbId, itemStatus)
                                                                                 VALUES ${addItemWiseSubToken};`
                                                                                : '';
                                                                            let sql_query_removesId = removed.length
                                                                                ?
                                                                                `UPDATE billing_itemWiseSubToken_data SET itemStatus = 'cancelled' WHERE iwbId IN (${removeJsonIds});
                                                                                INSERT INTO billing_modifiedKot_data(modifiedId, removedBy, iwbId, itemId, qty, unit, itemPrice, price, comment)
                                                                                VALUES ${addRemovedKotItem};`
                                                                                : '';
                                                                            let sql_query_updateModified = modifiedNewJson.length
                                                                                ?
                                                                                `UPDATE billing_itemWiseSubToken_data SET itemStatus = 'modified' WHERE iwbId IN (${updateJsonIds});`
                                                                                : '';
                                                                            let sql_query_addItemsAddons = addItemWiseAddonData.length ? `INSERT INTO billing_itemWiseAddon_data (iwaId, iwbId, addOnsId) VALUES ${addItemWiseAddonData.join(", ")}` : '';

                                                                            let sql_query_updateKotStatus = `${iwbIdAddArray.length ? sql_query_addItemsId : ''}
                                                                                                             ${removed.length ? sql_query_removesId : ''}
                                                                                                             ${modifiedNewJson.length ? sql_query_updateModified : ''}
                                                                                                             ${addItemWiseAddonData.length ? sql_query_addItemsAddons : ''}`;
                                                                            connection.query(sql_query_updateKotStatus, (err) => {
                                                                                if (err) {
                                                                                    console.error("Error inserting Item Wise Sub Token Id:", err);
                                                                                    connection.rollback(() => {
                                                                                        connection.release();
                                                                                        return res.status(500).send('Database Error');
                                                                                    });
                                                                                } else {
                                                                                    connection.commit((err) => {
                                                                                        if (err) {
                                                                                            console.error("Error committing transaction:", err);
                                                                                            connection.rollback(() => {
                                                                                                connection.release();
                                                                                                return res.status(500).send('Database Error');
                                                                                            });
                                                                                        } else {
                                                                                            const createJson = (items, status) =>
                                                                                                items.length ? items.map(e => ({
                                                                                                    iwbId: e.iwbId,
                                                                                                    itemId: e.itemId,
                                                                                                    inputCode: e.inputCode,
                                                                                                    itemName: e.itemName,
                                                                                                    preferredName: e.preferredName != null ? e.preferredName : null,
                                                                                                    qty: e.qty,
                                                                                                    unit: e.unit,
                                                                                                    itemPrice: e.itemPrice,
                                                                                                    price: e.price,
                                                                                                    comment: e.comment,
                                                                                                    kotItemStatus: status ? status : e.kotItemStatus ? e.kotItemStatus : null,
                                                                                                    addons: e.addons && typeof e.addons === 'object' ? e.addons : {}
                                                                                                })) : [];
                                                                                            const addedJson = createJson(added, 'new');
                                                                                            const removeJson = createJson(removed, 'cancelled');
                                                                                            const modifyJson = createJson(modifiedNewJson, 'modified');

                                                                                            const newAddJson = {
                                                                                                ...billData,
                                                                                                itemsData: addedJson,
                                                                                                tokenNo: billData.subTokenNumber ? billData.subTokenNumber : 'NA',
                                                                                                assignCaptain: billData.assignCaptain ? billData.assignCaptain : cashier,
                                                                                                billDate: new Date(currentDate).toLocaleDateString('en-GB'),
                                                                                                billTime: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
                                                                                                isEdit: true
                                                                                            }
                                                                                            const newRemoveJson = {
                                                                                                ...billData,
                                                                                                itemsData: removeJson,
                                                                                                tokenNo: billData.subTokenNumber ? billData.subTokenNumber : 'NA',
                                                                                                assignCaptain: billData.assignCaptain ? billData.assignCaptain : cashier,
                                                                                                billDate: new Date(currentDate).toLocaleDateString('en-GB'),
                                                                                                billTime: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
                                                                                                isEdit: true
                                                                                            }
                                                                                            const newModifiedJson = {
                                                                                                ...billData,
                                                                                                itemsData: modifyJson,
                                                                                                tokenNo: billData.subTokenNumber ? billData.subTokenNumber : 'NA',
                                                                                                assignCaptain: billData.assignCaptain ? billData.assignCaptain : cashier,
                                                                                                billDate: new Date(currentDate).toLocaleDateString('en-GB'),
                                                                                                billTime: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
                                                                                                isEdit: true
                                                                                            }
                                                                                            connection.release();
                                                                                            req?.io?.emit('updateTableView');
                                                                                            billData.isPrintKOT && addedJson.length ? req?.io?.emit(`print_Kot_${adminMacAddress}`, newAddJson) : "";
                                                                                            billData.isPrintKOT && removeJson.length ? req?.io?.emit(`print_Kot_${adminMacAddress}`, newRemoveJson) : "";
                                                                                            billData.isPrintKOT && modifyJson.length ? req?.io?.emit(`print_Kot_${adminMacAddress}`, newModifiedJson) : "";
                                                                                            return res.status(201).send("Success");
                                                                                        }
                                                                                    });
                                                                                }
                                                                            })
                                                                        }
                                                                    })
                                                                } else {
                                                                    connection.rollback(() => {
                                                                        connection.release();
                                                                        return res.status(401).send('No Change');
                                                                    });
                                                                }
                                                            }
                                                        })
                                                    }
                                                })
                                            }
                                        })
                                    } else {
                                        connection.rollback(() => {
                                            connection.release();
                                            return res.status(404).send('Admin Server Not Found');
                                        })
                                    }
                                }
                            })
                        }
                    } else {
                        connection.rollback(() => {
                            connection.release();
                            return res.status(404).send('Please Login First....!');
                        });
                    }
                }
            });
        } catch (error) {
            console.error('An error occurred', error);
            connection.rollback(() => {
                connection.release();
                return res.status(500).json('Internal Server Error');
            })
        }
    });
}

// Remove Sub Token Data By APP

const removeSubTokenDataByIdForApp = (req, res) => {
    pool2.getConnection((err, connection) => {
        if (err) {
            console.error("Error getting database connection:", err);
            return res.status(500).send('Database Error');
        }
        try {
            connection.beginTransaction((err) => {
                if (err) {
                    console.error("Error beginning transaction:", err);
                    connection.release();
                    return res.status(500).send('Database Error');
                } else {
                    let token;
                    token = req.headers ? req.headers.authorization.split(" ")[1] : null;
                    if (token) {
                        const decoded = jwt.verify(token, process.env.JWT_SECRET);
                        const cashier = decoded.id.firstName;

                        const subTokenId = req.query.subTokenId;
                        const billId = req.query.billId;
                        if (!subTokenId || !billId) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(404).send('Please Fill All The Fields..!');
                            })
                        } else {
                            let sql_query_getAdminId = `SELECT adminMacAddress FROM billing_admin_data`;
                            connection.query(sql_query_getAdminId, (err, macId) => {
                                if (err) {
                                    console.error("Error Get Pre Total Price:", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else {
                                    if (macId && macId.length) {
                                        const adminMacAddress = macId[0].adminMacAddress;
                                        let sql_query_ExistSubtokenData = `SELECT subTokenId FROM billing_subToken_data WHERE subTokenId = '${subTokenId}'`;
                                        connection.query(sql_query_ExistSubtokenData, (err, tkn) => {
                                            if (err) {
                                                console.error("Error Get Token Id:", err);
                                                connection.rollback(() => {
                                                    connection.release();
                                                    return res.status(500).send('Database Error');
                                                });
                                            } else {
                                                if (tkn && tkn.length) {
                                                    let sql_query_getPreTokenPrice = `SELECT SUM(bwi.price) AS preTotalPrice FROM billing_itemWiseSubToken_data  AS iwst
                                                                                      LEFT JOIN billing_billWiseItem_data AS bwi ON bwi.iwbId = iwst.iwbId
                                                                                      WHERE subTokenId = '${subTokenId}'`;
                                                    connection.query(sql_query_getPreTokenPrice, (err, prePrice) => {
                                                        if (err) {
                                                            console.error("Error Get Pre Total Price:", err);
                                                            connection.rollback(() => {
                                                                connection.release();
                                                                return res.status(500).send('Database Error');
                                                            });
                                                        } else {
                                                            const preTotalPrice = prePrice && prePrice[0].preTotalPrice ? prePrice[0].preTotalPrice : 0;
                                                            let sql_querry_updateBillData = `UPDATE
                                                                                                 billing_data
                                                                                             SET
                                                                                                 totalAmount = totalAmount - ${preTotalPrice},
                                                                                                 settledAmount = settledAmount - ${preTotalPrice}
                                                                                             WHERE
                                                                                                 billId = '${billId}'`;
                                                            connection.query(sql_querry_updateBillData, (err) => {
                                                                if (err) {
                                                                    console.error("Error Update new bill Price:", err);
                                                                    connection.rollback(() => {
                                                                        connection.release();
                                                                        return res.status(500).send('Database Error');
                                                                    });
                                                                } else {
                                                                    let modifiedId = `CONCAT('modified_',${Date.now()} + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),'_',ROW_NUMBER() OVER (ORDER BY (SELECT NULL)))`;
                                                                    let sql_query_removeSubToken = `INSERT INTO billing_modifiedKot_data(modifiedId, iwbId, itemId, qty, unit, itemPrice, price, comment)
                                                                                                    SELECT ${modifiedId}, iwbId, itemId, qty, unit, itemPrice, price, comment FROM billing_billWiseItem_data WHERE iwbId IN (SELECT COALESCE(billing_itemWiseSubToken_data.iwbId,NULL) FROM billing_itemWiseSubToken_data WHERE billing_itemWiseSubToken_data.subTokenId = '${subTokenId}');
                                                                                                    INSERT INTO billing_modifiedAddons_data(madId, iwbId, addOnsId)
                                                                                                    SELECT CONCAT('mad_', iwaId), iwbId, addOnsId FROM billing_itemWiseAddon_data WHERE iwbId IN (SELECT COALESCE(billing_itemWiseSubToken_data.iwbId,NULL) FROM billing_itemWiseSubToken_data WHERE billing_itemWiseSubToken_data.subTokenId = '${subTokenId}');
                                                                                                    UPDATE billing_itemWiseSubToken_data SET itemStatus = 'cancelled' WHERE billing_itemWiseSubToken_data.subTokenId = '${subTokenId}';
                                                                                                    UPDATE billing_subToken_data SET tokenStatus = 'cancelled', captain = '${cashier}' WHERE subTokenId = '${subTokenId}';
                                                                                                    DELETE FROM billing_billWiseItem_data WHERE iwbId IN (SELECT COALESCE(billing_itemWiseSubToken_data.iwbId,NULL) FROM billing_itemWiseSubToken_data WHERE billing_itemWiseSubToken_data.subTokenId = '${subTokenId}')`;
                                                                    connection.query(sql_query_removeSubToken, (err) => {
                                                                        if (err) {
                                                                            console.error("Error Delete Sub Token Item Data:", err);
                                                                            connection.rollback(() => {
                                                                                connection.release();
                                                                                return res.status(500).send('Database Error');
                                                                            });
                                                                        } else {
                                                                            let sql_query_getRemoveTokenData = `SELECT
                                                                                                                    bst.subTokenId AS subTokenId,
                                                                                                                    bst.tokenComment AS tokenComment,
                                                                                                                    bwtn.assignCaptain AS captain,
                                                                                                                    bwtn.tableNo AS tableNo,
                                                                                                                    DATE_FORMAT(bst.subTokenDate, '%d/%m/%Y') AS subTokenDate,
                                                                                                                    DATE_FORMAT(bst.creationDate, '%h:%i %p') AS createTime,
                                                                                                                    bst.subTokenNumber AS subTokenNumber,
                                                                                                                    bst.tokenStatus AS tokenStatus,
                                                                                                                    iwst.iwbId AS iwbId,
                                                                                                                    iwst.itemStatus AS kotItemStatus,
                                                                                                                    COALESCE(bwi.itemId, bmk.itemId) AS itemId,
                                                                                                                    imld.itemName AS itemName,
                                                                                                                    uwp.preferredName AS preferredName,
                                                                                                                    imld.itemCode AS inputCode,
                                                                                                                    COALESCE(bwi.qty, bmk.qty) AS qty,
                                                                                                                    COALESCE(bwi.unit, bmk.unit) AS unit,
                                                                                                                    COALESCE(bwi.itemPrice, bmk.itemPrice) AS itemPrice,
                                                                                                                    COALESCE(bwi.price, bmk.price) AS price,
                                                                                                                    COALESCE(bwi.comment, bmk.comment) AS comment,
                                                                                                                    iwad.iwaId,
                                                                                                                    iwad.addOnsId,
                                                                                                                    iad.addonsName,
                                                                                                                    iad.addonsGujaratiName,
                                                                                                                    iad.price AS addonPrice
                                                                                                                FROM
                                                                                                                    billing_subToken_data AS bst
                                                                                                                LEFT JOIN billing_itemWiseSubToken_data AS iwst ON iwst.subTokenId = bst.subTokenId
                                                                                                                LEFT JOIN billing_billWiseItem_data AS bwi ON bwi.iwbId = iwst.iwbId
                                                                                                                LEFT JOIN billing_modifiedKot_data AS bmk ON bmk.iwbId = iwst.iwbId
                                                                                                                LEFT JOIN item_menuList_data AS imld ON imld.itemId = COALESCE(bwi.itemId, bmk.itemId)
                                                                                                                LEFT JOIN item_unitWisePrice_data AS uwp ON uwp.itemId = COALESCE(bwi.itemId, bmk.itemId) AND uwp.unit = COALESCE(bwi.unit, bmk.unit) AND uwp.menuCategoryId = '${process.env.BASE_MENU}'
                                                                                                                LEFT JOIN billing_billWiseTableNo_data AS bwtn ON bwtn.billId = bst.billId
                                                                                                                LEFT JOIN (
                                                                                                                    SELECT iwaId, iwbId, addOnsId FROM billing_itemWiseAddon_data
                                                                                                                    UNION ALL
                                                                                                                    SELECT m.madId AS iwaId, m.iwbId, m.addOnsId
                                                                                                                    FROM billing_modifiedAddons_data AS m
                                                                                                                    WHERE NOT EXISTS (
                                                                                                                        SELECT 1 FROM billing_itemWiseAddon_data AS l
                                                                                                                        WHERE l.iwbId = m.iwbId AND l.addOnsId = m.addOnsId
                                                                                                                    )
                                                                                                                ) AS iwad ON iwad.iwbId = iwst.iwbId
                                                                                                                LEFT JOIN item_addons_data AS iad ON iad.addonsId = iwad.addOnsId
                                                                                                                WHERE bst.subTokenId = '${subTokenId}' AND bst.billId = '${billId}'`;
                                                                            connection.query(sql_query_getRemoveTokenData, (err, tknJson) => {
                                                                                if (err) {
                                                                                    console.error("Error Getting Remove Sub Token Data:", err);
                                                                                    connection.rollback(() => {
                                                                                        connection.release();
                                                                                        return res.status(500).send('Database Error');
                                                                                    });
                                                                                } else {
                                                                                    const tokenData = tknJson && tknJson.length ? tknJson : [];
                                                                                    const itemsByIwbId = {};
                                                                                    for (const row of tokenData) {
                                                                                        const id = row.iwbId;
                                                                                        if (!id) {
                                                                                            continue;
                                                                                        }
                                                                                        if (!itemsByIwbId[id]) {
                                                                                            itemsByIwbId[id] = {
                                                                                                iwbId: row.iwbId,
                                                                                                itemId: row.itemId,
                                                                                                inputCode: row.inputCode,
                                                                                                itemName: row.itemName,
                                                                                                preferredName: row.preferredName,
                                                                                                qty: row.qty,
                                                                                                unit: row.unit,
                                                                                                itemPrice: row.itemPrice,
                                                                                                price: row.price,
                                                                                                comment: row.comment,
                                                                                                kotItemStatus: row.kotItemStatus,
                                                                                                addons: {}
                                                                                            };
                                                                                        }
                                                                                        if (row.addOnsId) {
                                                                                            itemsByIwbId[id].addons[row.addOnsId] = {
                                                                                                iwaId: row.iwaId,
                                                                                                iwbId: row.iwbId,
                                                                                                addOnsId: row.addOnsId,
                                                                                                addonsName: row.addonsName,
                                                                                                addonsGujaratiName: row.addonsGujaratiName,
                                                                                                addonPrice: row.addonPrice
                                                                                            };
                                                                                        }
                                                                                    }
                                                                                    const itemsDataGrouped = Object.values(itemsByIwbId);
                                                                                    const tokenJson = {
                                                                                        subTokenId: tokenData ? tokenData[0].subTokenId : '',
                                                                                        assignCaptain: tokenData ? tokenData[0].captain : '',
                                                                                        tableNo: tokenData ? tokenData[0].tableNo : '',
                                                                                        billType: 'Dine In',
                                                                                        tokenNo: tokenData ? tokenData[0].subTokenNumber : '',
                                                                                        tokenStatus: tokenData ? tokenData[0].tokenStatus : '',
                                                                                        billDate: tokenData ? tokenData[0].subTokenDate : '',
                                                                                        billTime: tokenData ? tokenData[0].createTime : '',
                                                                                        tokenComment: tokenData ? tokenData[0].tokenComment : '',
                                                                                        totalPrice: itemsDataGrouped.reduce((sum, item) => sum + (Number(item.price) || 0), 0),
                                                                                        isDelete: true,
                                                                                        itemsData: itemsDataGrouped
                                                                                    };
                                                                                    let sql_query_chkExistToken = `SELECT subTokenId, billId FROM billing_subToken_data WHERE billId = '${billId}' AND tokenStatus = 'print'`;
                                                                                    connection.query(sql_query_chkExistToken, (err, chkTkn) => {
                                                                                        if (err) {
                                                                                            console.error("Error Delete Sub Token Item Data:", err);
                                                                                            connection.rollback(() => {
                                                                                                connection.release();
                                                                                                return res.status(500).send('Database Error');
                                                                                            });
                                                                                        } else {
                                                                                            if (chkTkn && chkTkn.length) {
                                                                                                connection.commit((err) => {
                                                                                                    if (err) {
                                                                                                        console.error("Error committing transaction:", err);
                                                                                                        connection.rollback(() => {
                                                                                                            connection.release();
                                                                                                            return res.status(500).send('Database Error');
                                                                                                        });
                                                                                                    } else {
                                                                                                        connection.release();
                                                                                                        req?.io?.emit('updateTableView');
                                                                                                        req?.io?.emit(`print_Kot_${adminMacAddress}`, tokenJson);
                                                                                                        return res.status(200).send('Token Deleted Successfully');
                                                                                                    }
                                                                                                });
                                                                                            } else {
                                                                                                let sql_query_getBillInfo = `SELECT bd.billId AS billId FROM billing_data AS bd WHERE bd.billId = '${billId}' AND bd.billType = 'Dine In'`;
                                                                                                connection.query(sql_query_getBillInfo, (err, billInfo) => {
                                                                                                    if (err) {
                                                                                                        console.error("Error get billInfo :", err);
                                                                                                        connection.rollback(() => {
                                                                                                            connection.release();
                                                                                                            return res.status(500).send('Database Error');
                                                                                                        });
                                                                                                    } else {
                                                                                                        if (billInfo && billInfo.length) {
                                                                                                            let sql_querry_cancelBillData = `UPDATE
                                                                                                                                                 billing_data
                                                                                                                                             SET
                                                                                                                                                 billPayType = 'CancelToken',
                                                                                                                                                 billStatus = 'CancelToken'
                                                                                                                                             WHERE billId = '${billId}'`;
                                                                                                            connection.query(sql_querry_cancelBillData, (err) => {
                                                                                                                if (err) {
                                                                                                                    console.error("Error Delete billData :", err);
                                                                                                                    connection.rollback(() => {
                                                                                                                        connection.release();
                                                                                                                        return res.status(500).send('Database Error');
                                                                                                                    });
                                                                                                                } else {
                                                                                                                    connection.commit((err) => {
                                                                                                                        if (err) {
                                                                                                                            console.error("Error committing transaction:", err);
                                                                                                                            connection.rollback(() => {
                                                                                                                                connection.release();
                                                                                                                                return res.status(500).send('Database Error');
                                                                                                                            });
                                                                                                                        } else {
                                                                                                                            connection.release();
                                                                                                                            req?.io?.emit('updateTableView');
                                                                                                                            req?.io?.emit(`print_Kot_${adminMacAddress}`, tokenJson);
                                                                                                                            return res.status(200).send('Table Bill Cancel Success');
                                                                                                                        }
                                                                                                                    });
                                                                                                                }
                                                                                                            });
                                                                                                        } else {
                                                                                                            connection.rollback(() => {
                                                                                                                connection.release();
                                                                                                                return res.status(404).send('billId Not Found...!');
                                                                                                            })
                                                                                                        }
                                                                                                    }
                                                                                                })
                                                                                            }
                                                                                        }
                                                                                    })
                                                                                }
                                                                            })
                                                                        }
                                                                    })
                                                                }
                                                            })
                                                        }
                                                    })
                                                } else {
                                                    connection.rollback(() => {
                                                        connection.release();
                                                        return res.status(401).send('subTokenId Not Found');
                                                    });
                                                }
                                            }
                                        });
                                    } else {
                                        connection.rollback(() => {
                                            connection.release();
                                            return res.status(404).send('Admin Server Not Found');
                                        })
                                    }
                                }
                            })
                        }
                    } else {
                        connection.rollback(() => {
                            connection.release();
                            return res.status(404).send('Please Login First....!');
                        });
                    }
                }
            });
        } catch (error) {
            console.error('An error occurred', error);
            connection.rollback(() => {
                connection.release();
                return res.status(500).json('Internal Server Error');
            })
        }
    });
}

// Check Is Table Empty Or Not

const isTableEmpty = (req, res) => {
    try {
        const tableNo = req.query.tableNo;
        const areaId = req.query.areaId;
        if (!tableNo) {
            return res.status(404).send('Please Enter Table Number')
        } else if (tableNo.length > 4) {
            return res.status(404).send('Please Enter Short Names')
        } else {
            let sql_query_chkTableIsEmpty = `SELECT tableId, tableNo FROM billing_DineInTable_data WHERE areaId = '${areaId}' AND tableNo = '${tableNo}' AND billId IS NOT NULL`;
            pool.query(sql_query_chkTableIsEmpty, (err, table) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    if (table && table.length) {
                        return res.status(401).send(`Table No. ${tableNo} is Not Available in`);
                    } else {
                        return res.status(200).send('Available');
                    }
                }
            });
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Print Table Bill For App

const printTableBillForApp = (req, res) => {
    pool2.getConnection((err, connection) => {
        if (err) {
            console.error("Error getting database connection:", err);
            return res.status(500).send('Database Error');
        }
        try {
            connection.beginTransaction((err) => {
                if (err) {
                    console.error("Error beginning transaction:", err);
                    connection.release();
                    return res.status(500).send('Database Error');
                } else {
                    let token;
                    token = req.headers ? req.headers.authorization.split(" ")[1] : null;
                    if (token) {
                        const decoded = jwt.verify(token, process.env.JWT_SECRET);
                        const cashier = decoded.id.firstName;
                        const branchId = decoded.id.branchId;
                        const billId = req.query.billId;
                        if (!billId) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(404).send('billId Not Found');
                            });
                        } else {
                            let sql_query_updateTableStatus = `UPDATE billing_data SET billStatus = 'print' WHERE billId = '${billId}'`;
                            let sql_query_updatePrintDateTime = `UPDATE billing_billWiseTableNo_data SET printTime = NOW() WHERE billId = '${billId}'`;
                            let sql_query_updateBoth = `${sql_query_updateTableStatus};
                                                    ${sql_query_updatePrintDateTime}`;
                            connection.query(sql_query_updateBoth, (err) => {
                                if (err) {
                                    console.error("Error updating table status and print time:", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else {
                                    // Check isOfficial from billing_category_data
                                    let sql_query_checkIsOfficial = `SELECT isOfficial FROM billing_branchWiseCategory_data WHERE categoryId = 'dineIn' AND branchId = '${branchId}'`;
                                    connection.query(sql_query_checkIsOfficial, (err, isOfficialResult) => {
                                        if (err) {
                                            console.error("Error checking isOfficial:", err);
                                            connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send('Database Error');
                                            });
                                        } else {
                                            const isOfficial = isOfficialResult && isOfficialResult.length && isOfficialResult[0].isOfficial ? isOfficialResult[0].isOfficial : false;

                                            if (isOfficial) {
                                                const currentDate = getCurrentDate();
                                                const resetStartDateExpr = `STR_TO_DATE(
                                                                            CONCAT(
                                                                                CASE
                                                                                    WHEN DATE(STR_TO_DATE('${currentDate}', '%b %d %Y')) < STR_TO_DATE(
                                                                                        CONCAT(YEAR(STR_TO_DATE('${currentDate}', '%b %d %Y')), '-', frm.resetDate),
                                                                                        '%Y-%m-%d'
                                                                                    )
                                                                                    THEN YEAR(STR_TO_DATE('${currentDate}', '%b %d %Y')) - 1
                                                                                    ELSE YEAR(STR_TO_DATE('${currentDate}', '%b %d %Y'))
                                                                                END,
                                                                                '-',
                                                                                frm.resetDate
                                                                            ),
                                                                            '%Y-%m-%d'
                                                                        )`;
                                                let sql_query_chkOfficial = `SELECT billId, billNumber FROM billing_Official_data WHERE billId = '${billId}';
                                                                         SELECT COALESCE(MAX(bod.billNumber), 0) AS officialLastBillNo
                                                                         FROM billing_Official_data bod
                                                                         CROSS JOIN (SELECT COALESCE(resetDate, '04-01') AS resetDate FROM billing_firm_data WHERE firmId = (SELECT firmId FROM billing_category_data WHERE categoryId = 'dineIn') LIMIT 1) AS frm
                                                                         WHERE bod.firmId = (SELECT firmId FROM billing_category_data WHERE categoryId = 'dineIn')
                                                                         AND bod.billDate >= ${resetStartDateExpr}
                                                                         FOR UPDATE;`;
                                                connection.query(sql_query_chkOfficial, (err, chkExist) => {
                                                    if (err) {
                                                        console.error("Error check official bill exist or not:", err);
                                                        connection.rollback(() => {
                                                            connection.release();
                                                            return res.status(500).send('Database Error');
                                                        });
                                                    } else {
                                                        const isExist = chkExist && chkExist[0] && chkExist[0].length ? true : false;
                                                        if (!isExist) {
                                                            const officialLastBillNo = chkExist && chkExist[1] ? chkExist[1][0].officialLastBillNo : 0;
                                                            const nextOfficialBillNo = officialLastBillNo + 1;

                                                            let sql_query_addOfficial = `INSERT INTO billing_Official_data(billId, billNumber, firmId, branchId, cashier, menuStatus, billType, billPayType, discountType, discountValue, totalDiscount, totalAmount, settledAmount, billComment, billDate, billStatus)
                                                                                         SELECT billId, ${nextOfficialBillNo}, firmId, branchId, cashier, menuStatus, billType, billPayType, discountType, discountValue, totalDiscount, totalAmount, settledAmount, billComment, billDate, 'print' FROM billing_data WHERE billId = '${billId}'`;
                                                            connection.query(sql_query_addOfficial, (err) => {
                                                                if (err) {
                                                                    console.error("Error adding official bill data:", err);
                                                                    connection.rollback(() => {
                                                                        connection.release();
                                                                        return res.status(500).send('Database Error');
                                                                    });
                                                                } else {
                                                                    // Proceed to get bill data
                                                                    getBillDataAndCommit();
                                                                }
                                                            });
                                                        } else {
                                                            // Bill already exists in official data, proceed to get bill data
                                                            getBillDataAndCommit();
                                                        }
                                                    }
                                                });
                                            } else {
                                                // isOfficial is false, proceed directly to get bill data
                                                getBillDataAndCommit();
                                            }
                                        }
                                    });
                                }
                            });

                            // Function to get bill data and commit transaction
                            function getBillDataAndCommit() {
                                let sql_query_getBillingData = `SELECT 
                                                                    bd.billId AS billId, 
                                                                    bd.billNumber AS billNumber,
                                                                    COALESCE(bod.billNumber, CONCAT('C', bcd.billNumber), 'Not Available') AS officialBillNo,
                                                                    CASE
                                                                        WHEN bod.billNumber IS NOT NULL THEN true
                                                                        WHEN bcd.billNumber IS NOT NULL THEN true
                                                                        ELSE false
                                                                    END AS isOfficial,
                                                                    CASE
                                                                        WHEN bd.billType = 'Pick Up' THEN CONCAT('P',btd.tokenNo)
                                                                        WHEN bd.billType = 'Delivery' THEN CONCAT('D',btd.tokenNo)
                                                                        WHEN bd.billType = 'Dine In' THEN CONCAT('R',btd.tokenNo)
                                                                        ELSE NULL
                                                                    END AS tokenNo,
                                                                    CASE
                                                                        WHEN bd.billPayType = 'online' THEN bwu.onlineId
                                                                        ELSE NULL
                                                                    END AS onlineId,
                                                                    bd.firmId AS firmId, 
                                                                    bd.cashier AS cashier, 
                                                                    bd.menuStatus AS menuStatus, 
                                                                    bd.billType AS billType, 
                                                                    bd.billPayType AS billPayType, 
                                                                    bd.discountType AS discountType, 
                                                                    bd.discountValue AS discountValue, 
                                                                    bd.totalDiscount AS totalDiscount, 
                                                                    bd.totalAmount AS subTotal, 
                                                                    bd.settledAmount AS settledAmount, 
                                                                    bd.billComment AS billComment, 
                                                                    DATE_FORMAT(bd.billDate,'%d/%m/%Y') AS billDate,
                                                                    bd.billStatus AS billStatus,
                                                                    DATE_FORMAT(bd.billCreationDate,'%h:%i %p') AS billTime,
                                                                    bcgd.billFooterNote AS footerBill,
                                                                    bcgd.appriciateLine AS appriciateLine
                                                                FROM 
                                                                    billing_data AS bd
                                                                LEFT JOIN billing_Official_data AS bod ON bod.billId = bd.billId
                                                                LEFT JOIN billing_Complimentary_data AS bcd ON bcd.billId = bd.billId
                                                                LEFT JOIN billing_token_data AS btd ON btd.billId = bd.billId
                                                                LEFT JOIN billing_firm_data AS bfd ON bfd.firmId = bd.firmId
                                                                LEFT JOIN billing_billWiseUpi_data AS bwu ON bwu.billId = bd.billId
                                                                LEFT JOIN billing_branchWiseCategory_data AS bcgd ON bcgd.categoryId = 'dineIn' AND bcgd.branchId = '${branchId}'
                                                                WHERE bd.billId = '${billId}'`;
                                let sql_query_getBillwiseItem = `SELECT
                                                                     bwid.iwbId AS iwbId,
                                                                     bwid.itemId AS itemId,
                                                                     imd.itemName AS itemName,
                                                                     imd.itemCode AS inputCode,
                                                                     SUM(bwid.qty) AS qty,
                                                                     bwid.unit AS unit,
                                                                     bwid.itemPrice AS itemPrice,
                                                                     bwid.price AS price,
                                                                     bwid.comment AS comment
                                                                 FROM
                                                                     billing_billWiseItem_data AS bwid
                                                                 INNER JOIN item_menuList_data AS imd ON imd.itemId = bwid.itemId
                                                                 WHERE bwid.billId = '${billId}'`;
                                let sql_query_getItemWiseAddons = `SELECT
                                                                   iwad.iwaId AS iwaId,
                                                                   iwad.iwbId AS iwbId,
                                                                   iwad.addOnsId AS addOnsId,
                                                                   iad.addonsName AS addonsName,
                                                                   iad.addonsGujaratiName AS addonsGujaratiName,
                                                                   iad.price AS addonPrice
                                                               FROM
                                                                   billing_itemWiseAddon_data AS iwad
                                                               LEFT JOIN item_addons_data AS iad ON iad.addonsId = iwad.addOnsId
                                                               WHERE iwad.iwbId IN(SELECT COALESCE(bwid.iwbId, NULL) FROM billing_billWiseItem_data AS bwid WHERE bwid.billId = '${billId}')`;
                                let sql_query_getCustomerInfo = `SELECT
                                                                     bwcd.bwcId AS bwcId,
                                                                     bwcd.customerId AS customerId,
                                                                     bwcd.mobileNo AS mobileNo,
                                                                     bwcd.addressId AS addressId,
                                                                     bwcd.address AS address,
                                                                     bwcd.locality AS locality,
                                                                     bwcd.customerName AS customerName
                                                                 FROM
                                                                     billing_billWiseCustomer_data AS bwcd
                                                                 WHERE bwcd.billId = '${billId}'`;
                                let sql_query_getFirmData = `SELECT 
                                                                firmId, 
                                                                firmName, 
                                                                gstNumber, 
                                                                firmAddress, 
                                                                pincode, 
                                                                firmMobileNo, 
                                                                otherMobileNo 
                                                             FROM 
                                                                billing_firm_data 
                                                             WHERE 
                                                                firmId = (SELECT firmId FROM billing_data WHERE billId = '${billId}')`;
                                let sql_query_getTableData = `SELECT
                                                                tableNo,
                                                                assignCaptain
                                                              FROM
                                                                billing_billWiseTableNo_data
                                                              WHERE billId = '${billId}'`;
                                let sql_query_getSubTokens = `SELECT subTokenNumber FROM billing_subToken_data WHERE billId = '${billId}'`;
                                let sql_query_getAdminId = `SELECT adminMacAddress FROM billing_admin_data`;

                                const sql_query_getBillData = `${sql_query_getBillingData};
                                                              ${sql_query_getBillwiseItem};
                                                              ${sql_query_getFirmData};
                                                              ${sql_query_getItemWiseAddons};
                                                              ${sql_query_getCustomerInfo};
                                                              ${sql_query_getTableData};
                                                              ${sql_query_getSubTokens};
                                                              ${sql_query_getAdminId}`;

                                connection.query(sql_query_getBillData, (err, billData) => {
                                    if (err) {
                                        console.error("An error occurred in SQL Query:", err);
                                        connection.rollback(() => {
                                            connection.release();
                                            return res.status(500).send('Database Error');
                                        });
                                    } else {
                                        connection.commit((err) => {
                                            if (err) {
                                                console.error("Error committing transaction:", err);
                                                connection.rollback(() => {
                                                    connection.release();
                                                    return res.status(500).send('Database Error');
                                                });
                                            } else {
                                                const macId = billData && billData[7] ? billData[7] : [];
                                                if (macId && macId.length) {
                                                    const adminMacAddress = macId[0].adminMacAddress;
                                                    const itemsData = billData && billData[1] ? billData[1] : [];
                                                    const addonsData = billData && billData[3] ? billData[3] : [];

                                                    const newItemJson = itemsData.map(item => {
                                                        const itemAddons = addonsData.filter(addon => addon.iwbId === item.iwbId);
                                                        return {
                                                            ...item,
                                                            addons: Object.fromEntries(itemAddons.map(addon => [addon.addOnsId, addon])),
                                                            addonPrice: itemAddons.reduce((sum, { price }) => sum + price, 0)
                                                        };
                                                    });
                                                    const json = {
                                                        ...billData[0][0],
                                                        itemsData: newItemJson,
                                                        firmData: billData && billData[2] ? billData[2][0] : [],
                                                        ...({ customerDetails: billData && billData[4][0] ? billData[4][0] : '' }),
                                                        ...({ tableInfo: billData[5][0] }),
                                                        subTokens: billData[6].map(item => item.subTokenNumber).sort((a, b) => a - b).join(", "),
                                                        tableNo: billData[5][0].tableNo ? billData[5][0].tableNo : 0
                                                    }
                                                    connection.release();
                                                    req?.io?.emit('updateTableView');
                                                    req?.io?.emit(`print_Bill_${adminMacAddress}`, json);
                                                    return res.status(200).send(json);
                                                } else {
                                                    connection.release();
                                                    return res.status(404).send('Main Server Not Found');
                                                }
                                            }
                                        });
                                    }
                                });
                            }
                        }
                    } else {
                        connection.rollback(() => {
                            connection.release();
                            return res.status(404).send('Please Login First....!');
                        });
                    }
                }
            });
        } catch (error) {
            console.error('An error occurred', error);
            connection.rollback(() => {
                connection.release();
                return res.status(500).json('Internal Server Error');
            });
        }
    });
}

// Get Server Ip For Captain App

const findServerIpByApp = (req, res) => {
    try {
        return res.status(200).send("Success");
    } catch (error) {
        console.error('An error occurred', error);
        return res.status(500).json('Internal Server Error');
    }
}

module.exports = {
    addDineInOrderByApp,
    getSubTokensByBillIdForApp,
    removeSubTokenDataByIdForApp,
    updateSubTokenDataByIdForApp,
    isTableEmpty,
    printTableBillForApp,
    findServerIpByApp,
    getSubTokenDataByIdForApp
}