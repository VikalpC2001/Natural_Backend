const pool = require('../../database');
const jwt = require("jsonwebtoken");
const pool2 = require('../../databasePool');

// Get Date Function 4 Hour

function getCurrentDate() {
    const now = new Date();
    const hours = now.getHours();

    if (hours <= 4) { // If it's 4 AM or later, increment the date
        now.setDate(now.getDate() - 1);
    }
    return now.toDateString().slice(4, 15);
}

// Grouped Item Wise Addons

function groupItemsWithAddons(data) {
    const grouped = {};

    data.forEach(row => {
        const id = row.iwbId;

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
                kotItemStatus: row.kotItemStatus || null,
                addons: {}
            };
        }

        // If addon exists, add inside addons object
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

            // Compare only qty, price, and comment fields
            return (
                item.qty !== json2Item.qty ||
                item.unit !== json2Item.unit ||
                item.itemPrice !== json2Item.itemPrice ||
                item.price !== json2Item.price ||
                ((item.comment ? item.comment : '') !== (json2Item.comment ? json2Item.comment : ''))
            );
        })
        .map(item => ({
            old: item, // Return the entire old object
            new: json2Map.get(item.iwbId) // Return the entire new object
        }));

    return { added, removed, modified };
}

// Get All Table View

const getAllTableView = (req, res) => {
    try {
        const sql_query_getDetails = `SELECT
                                          dt.tableId AS tableId,
                                          dt.areaId AS areaId,
                                          dia.areaName AS areaName,
                                          dt.tableNo AS tableNo,
                                          IFNULL(CONCAT(dia.prefix, ' ', dt.tableNo), dt.tableNo) AS displayTableNo,
                                          dt.billId AS billId,
                                          bwt.assignCaptain AS assignCaptain,
                                          CASE 
                                              WHEN dt.billId IS NULL THEN 'blank'
                                              ELSE bd.billStatus
                                          END AS tableStatus,
                                          dt.isFixed AS isFixed,
                                          bd.billPayType AS billPayType,
                                          bd.discountType AS discountType,
                                          COALESCE(bd.discountValue, 0) AS discountValue,
                                          COALESCE(bd.totalDiscount, 0) AS totalDiscount,
                                          COALESCE(bd.totalAmount, 0) AS totalAmount,
                                          COALESCE(bd.settledAmount, 0) AS billAmt,
                                          TIMESTAMPDIFF(MINUTE, bd.billCreationDate, NOW()) AS tableStartTime,
                                          IF(bd.billStatus = 'print',
                                                CONCAT(
                                                    'upi://pay?pa=', upi.upiId,
                                                    '&pn=', upi.holderName,
                                                    '&tn=Restaurent Bill&am=',
                                                    COALESCE(bd.settledAmount, 0)
                                                ),
                                                NULL
                                            ) 
                                          AS upiLink
                                      FROM
                                          billing_DineInTable_data AS dt
                                      LEFT JOIN billing_data AS bd ON bd.billId = dt.billId
                                      LEFT JOIN billing_billWiseTableNo_data AS bwt ON bwt.billId = dt.billId
                                      LEFT JOIN billing_onlineUPI_data AS upi ON upi.isDefault = 1
                                      LEFT JOIN billing_dineInArea_data AS dia ON dt.areaId = dia.areaId
                                      ORDER BY dia.displayRank, CAST(dt.tableNo AS UNSIGNED), dt.tableNo`;
        pool.query(sql_query_getDetails, (err, data) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');;
            } else {
                const result = data.reduce((acc, curr) => {
                    (acc[curr.areaName] = acc[curr.areaName] || []).push(curr);
                    return acc;
                }, {});
                return res.status(200).send(result);
            }
        })
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Get All Sub Token By BillId

const getSubTokensByBillId = async (req, res) => {
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
                                          WHERE bst.billId = '${billId}'
                                          ORDER BY bst.creationDate DESC, bst.subTokenNumber DESC;
`
            pool.query(sql_queries_getDetails, (err, data) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    const result = data.reduce((acc, row) => {

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

                    const cleanedData = result.map(token => {
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
                    return res.status(200).send(cleanedData);
                }
            });
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Add Dine In Order

const addDineInOrder = (req, res) => {
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

                        if (!branchId || !billData.areaId || !billData.tableNo || !billData.firmId || !billData.subTotal || !billData.settledAmount || !billData.itemsData.length) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(404).send('Please Fill All The Fields..!');
                            })
                        } else {
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
                                                                                                   SELECT COALESCE(MAX(subTokenNumber),0) AS lastSubTokenNo FROM billing_subtoken_data WHERE branchId = '${branchId}' AND subTokenDate = STR_TO_DATE('${currentDate}','%b %d %Y') FOR UPDATE;`;
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
                                                                                            '${billData.firmId}',
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

// Remove SubToken Data

const removeSubTokenDataById = (req, res) => {
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
                                                                                    return res.status(200).send('Token Deleted Successfully');
                                                                                }
                                                                            });
                                                                        } else {
                                                                            let sql_query_chkIsFixed = `SELECT tableNo, isFixed FROM billing_DineInTable_data WHERE tableNo = (SELECT tableNo FROM billing_billWiseTableNo_data WHERE billId = '${billId}') AND billId = '${billId}'`;
                                                                            connection.query(sql_query_chkIsFixed, (err, chkExist) => {
                                                                                if (err) {
                                                                                    console.error("Error check table is fixed or not:", err);
                                                                                    connection.rollback(() => {
                                                                                        connection.release();
                                                                                        return res.status(500).send('Database Error');
                                                                                    });
                                                                                } else {
                                                                                    const isTableFixed = chkExist && chkExist.length ? chkExist[0].isFixed : true;
                                                                                    const tableNo = chkExist && chkExist.length ? chkExist[0].tableNo : true;
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
                                                                                                let sql_querry_removeBillData = `DELETE FROM billing_data WHERE billId = '${billId}'`;
                                                                                                connection.query(sql_querry_removeBillData, (err) => {
                                                                                                    if (err) {
                                                                                                        console.error("Error Delete billData :", err);
                                                                                                        connection.rollback(() => {
                                                                                                            connection.release();
                                                                                                            return res.status(500).send('Database Error');
                                                                                                        });
                                                                                                    } else {
                                                                                                        let sql_query_sattledData = isTableFixed == true
                                                                                                            ?
                                                                                                            `UPDATE billing_DineInTable_data SET billId = null WHERE tableNo = '${tableNo}' AND billId = '${billId}';`
                                                                                                            :
                                                                                                            `DELETE FROM billing_DineInTable_data WHERE billId = '${billId}' AND tableNo = '${tableNo}';`;
                                                                                                        connection.query(sql_query_sattledData, (err) => {
                                                                                                            if (err) {
                                                                                                                console.error("Error in sattled Data:", err);
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
                                                                                                                        return res.status(200).send('Table Bill Cancel Success');
                                                                                                                    }
                                                                                                                });
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
                                    } else {
                                        connection.rollback(() => {
                                            connection.release();
                                            return res.status(401).send('subTokenId Not Found');
                                        });
                                    }
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

// Update Sub Token Data

const updateSubTokenDataById = (req, res) => {
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
                        if (!branchId || !billData.subTokenId || !billData.billId || !billData.subTokenNumber || !billData.settledAmount || !billData.subTotal || !billData.itemsData.length) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(404).send('Please Fill All The Fields..!');
                            })
                        } else {
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
                                                    const oldResult = groupItemsWithAddons(oldJson);
                                                    const json1 = Object.values(JSON.parse(JSON.stringify(oldResult)));
                                                    const json2 = Object.values(JSON.parse(JSON.stringify(billData.itemsData)));
                                                    console.log("111", json1, "222", json2)
                                                    const { added, removed, modified } = compareJson(json1, json2);

                                                    if (added.length || removed.length || modified.length) {
                                                        const modifiedNewJson = modified.map(({ new: newItem }) => newItem);

                                                        const addBillWiseItemData = [];
                                                        const addItemWiseAddonData = [];
                                                        let iwbIdAddArray = []

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
                                                            `UPDATE billing_billWiseItem_data
                                                                SET 
                                                                qty = CASE iwbId ` +
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
                                                            : `SELECT * FROM user_details WHERE userId = '0'`;

                                                        let sql_query_adjustItem = `${added.length ? `INSERT INTO billing_billWiseItem_data(iwbId, billId, branchId, itemId, qty, unit, itemPrice, price, comment, billType, billPayType, billStatus, billDate)
                                                                                                      VALUES ${addBillWiseItemData};` : ''}
                                                                                                      ${removed.length ? `DELETE FROM billing_billWiseItem_data WHERE iwbId IN (${removeJsonIds});` : ''}
                                                                                                      ${modifiedNewJson.length ? `${updateQuery};` : `${updateQuery};`}`;
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
                                                                                        preferredName: e.preferredName,
                                                                                        qty: e.qty,
                                                                                        unit: e.unit,
                                                                                        itemPrice: e.itemPrice,
                                                                                        price: e.price,
                                                                                        comment: e.comment,
                                                                                        kotItemStatus: status ? status : e.kotItemStatus ? e.kotItemStatus : null,
                                                                                        addons: e.addons ? e.addons : {}
                                                                                    })) : [];
                                                                                const addedJson = createJson(added, 'new');
                                                                                const removeJson = createJson(removed, 'cancelled');
                                                                                const modifyJson = createJson(modifiedNewJson, 'modified');
                                                                                const existItemData = createJson(billData.itemsData)
                                                                                const mergedJson = [...addedJson, ...removeJson, ...modifyJson];

                                                                                const newMergeJson = (json1, json2) => {
                                                                                    json1.forEach(item1 => {
                                                                                        const index = json2.findIndex(item2 => item2.itemId === item1.itemId && item2.unit === item1.unit);

                                                                                        if (index !== -1) {
                                                                                            // If a match is found, replace the object in json2 with the one from json1
                                                                                            json2[index] = item1;
                                                                                        } else {
                                                                                            // If no match is found, push the object from json1 to json2
                                                                                            json2.push(item1);
                                                                                        }
                                                                                    });
                                                                                    return json2;
                                                                                };
                                                                                const newItemsData = newMergeJson(mergedJson, existItemData);

                                                                                const sendJson = {
                                                                                    ...billData,
                                                                                    itemsData: newItemsData,
                                                                                    assignCaptain: billData.assignCaptain ? billData.assignCaptain : cashier,
                                                                                    billDate: new Date(currentDate).toLocaleDateString('en-GB'),
                                                                                    billTime: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
                                                                                }
                                                                                console.log(sendJson);
                                                                                connection.release();
                                                                                req?.io?.emit('updateTableView');
                                                                                return res.status(201).send(sendJson);
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

// Print Table Bill

const printTableBill = (req, res) => {
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
                        const currentDate = getCurrentDate();
                        const currentDateMD = `DATE_FORMAT(STR_TO_DATE('${currentDate}', '%b %d %Y'), '%m-%d')`;
                        let sql_query_chkOfficial = `SELECT billId, billNumber FROM billing_Official_data WHERE billId = '${billId}';
                                                     SELECT IF(COUNT(*) = 0, 0, MAX(billNumber)) AS officialLastBillNo FROM billing_Official_data bod CROSS JOIN (SELECT COALESCE(resetDate, '04-01') AS resetDate FROM billing_firm_data WHERE firmId = (SELECT firmId FROM billing_branchWiseCategory_data WHERE categoryId = 'dineIn' AND branchId = '${branchId}') LIMIT 1) AS frm WHERE bod.firmId = (SELECT firmId FROM billing_branchWiseCategory_data WHERE categoryId = 'dineIn' AND branchId = '${branchId}') AND (${currentDateMD} < frm.resetDate OR (${currentDateMD} >= frm.resetDate AND DATE_FORMAT(bod.billDate, '%m-%d') >= frm.resetDate AND DATE_FORMAT(bod.billCreationDate, '%m-%d') >= frm.resetDate)) FOR UPDATE;`;
                        connection.query(sql_query_chkOfficial, (err, chkExist) => {
                            if (err) {
                                console.error("Error check official bill exist or not:", err);
                                connection.rollback(() => {
                                    connection.release();
                                    return res.status(500).send('Database Error');
                                });
                            } else {
                                const officialLastBillNo = chkExist && chkExist[1] ? chkExist[1][0].officialLastBillNo : 0;
                                const nextOfficialBillNo = officialLastBillNo + 1;

                                let sql_query_addOfficial = `INSERT INTO billing_Official_data(billId, billNumber, firmId, branchId, cashier, menuStatus, billType, billPayType, discountType, discountValue, totalDiscount, totalAmount, settledAmount, billComment, billDate, billStatus)
                                                         SELECT billId, ${nextOfficialBillNo}, firmId, branchId, cashier, menuStatus, billType, billPayType, discountType, discountValue, totalDiscount, totalAmount, settledAmount, billComment, billDate, 'print' FROM billing_data WHERE billId = '${billId}'`;
                                connection.query(sql_query_addOfficial, (err, raw) => {
                                    if (err) {
                                        console.error("An error occurred in SQL Queery", err);
                                        connection.rollback(() => {
                                            connection.release();
                                            return res.status(500).send('Database Error');
                                        });
                                    } else {
                                        let sql_query_getBillingData = `SELECT 
                                                                        bd.billId AS billId, 
                                                                        bd.billNumber AS billNumber,
                                                                        COALESCE(bod.billNumber, CONCAT('C', bcd.billNumber), 'Not Available') AS officialBillNo,
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
                                                                        bwcd.billFooterNote AS footerBill,
                                                                        bwcd.appriciateLine AS appriciateLine
                                                                    FROM 
                                                                        billing_data AS bd
                                                                    LEFT JOIN billing_Official_data AS bod ON bod.billId = bd.billId
                                                                    LEFT JOIN billing_Complimentary_data AS bcd ON bcd.billId = bd.billId
                                                                    LEFT JOIN billing_token_data AS btd ON btd.billId = bd.billId
                                                                    LEFT JOIN billing_firm_data AS bfd ON bfd.firmId = bd.firmId
                                                                    LEFT JOIN billing_billWiseUpi_data AS bwu ON bwu.billId = bd.billId
                                                                    LEFT JOIN billing_category_data AS bcgd ON bcgd.categoryName = bd.billType
                                                                    LEFT JOIN billing_branchWiseCategory_data AS bwcd ON bwcd.categoryId = bcgd.categoryId
                                                                    WHERE bd.billId = '${billId}'`;
                                        let sql_query_getBillwiseItem = `SELECT
                                                         bwid.iwbId AS iwbId,
                                                         bwid.itemId AS itemId,
                                                         imd.itemName AS itemName,
                                                         imd.itemCode AS inputCode,
                                                         SUM(bwid.qty) AS qty,
                                                         bwid.unit AS unit,
                                                         bwid.itemPrice AS itemPrice,
                                                         SUM(bwid.price) AS price,
                                                         bwid.comment AS comment
                                                     FROM
                                                         billing_billWiseItem_data AS bwid
                                                     INNER JOIN item_menuList_data AS imd ON imd.itemId = bwid.itemId
                                                     WHERE bwid.billId = '${billId}'
                                                     GROUP BY bwid.itemId, bwid.unit`;
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
                                                                        bwtn.tableNo AS tableNo,
                                                                        bwtn.areaId AS areaId,
                                                                        bwtn.assignCaptain AS assignCaptain,
                                                                        dia.areaName AS areaName,
                                                                        IFNULL(CONCAT(dia.prefix, ' ', bwtn.tableNo), bwtn.tableNo) AS displayTableNo
                                                                      FROM
                                                                        billing_billWiseTableNo_data AS bwtn
                                                                      LEFT JOIN billing_dineInArea_data dia ON dia.areaId = bwtn.areaId
                                                                      WHERE billId = '${billId}'`;
                                        let sql_query_getSubTokens = `SELECT subTokenNumber FROM billing_subToken_data WHERE billId = '${billId}'`;

                                        const sql_query_getBillData = `${sql_query_getBillingData};
                                                   ${sql_query_getBillwiseItem};
                                                   ${sql_query_getFirmData};
                                                   ${sql_query_getItemWiseAddons};
                                                   ${sql_query_getCustomerInfo};
                                                   ${sql_query_getTableData};
                                                   ${sql_query_getSubTokens}`;

                                        let sql_query_updateTableStatus = `UPDATE billing_data SET billStatus = 'print' WHERE billId = '${billId}'`;
                                        let sql_query_updatePrintDateTime = `UPDATE billing_billWiseTableNo_data SET printTime = NOW() WHERE billId = '${billId}'`;
                                        connection.query(sql_query_updateTableStatus, (err, raw) => {
                                            if (err) {
                                                console.error("An error occurred in SQL Queery", err);
                                                connection.rollback(() => {
                                                    connection.release();
                                                    return res.status(500).send('Database Error');
                                                });
                                            } else {
                                                connection.query(sql_query_updatePrintDateTime, (err, time) => {
                                                    if (err) {
                                                        console.error("An error occurred in SQL Queery", err);
                                                        connection.rollback(() => {
                                                            connection.release();
                                                            return res.status(500).send('Database Error');
                                                        });
                                                    } else {
                                                        connection.query(sql_query_getBillData, (err, billData) => {
                                                            if (err) {
                                                                console.error("An error occurred in SQL Queery", err);
                                                                connection.rollback(() => {
                                                                    connection.release();
                                                                    return res.status(500).send('Database Error');
                                                                });
                                                            } else {
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
                                                                        return res.status(200).send(json);
                                                                    }
                                                                });
                                                            }
                                                        });
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

// Update Bill Data After Print Table

const updateDineInBillData = (req, res) => {
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
                        const isComplimentary = billData.billPayType == 'complimentary' ? true : false;

                        if (!branchId || !billData.billId || !billData.subTotal || !billData.settledAmount || !billData.billPayType || !billData.billStatus || !billData.tableNo || !billData.itemsData) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(404).send('Please Fill All The Fields..!');
                            })
                        } else {
                            const currentDateMD = `DATE_FORMAT(STR_TO_DATE('${currentDate}', '%b %d %Y'), '%m-%d')`;
                            let sql_query_chkOfficial = `SELECT billId, billNumber FROM billing_Official_data WHERE billId = '${billData.billId}';
                                                         SELECT billId, billNumber FROM billing_Complimentary_data WHERE billId = '${billData.billId}';
                                                         SELECT IF(COUNT(*) = 0, 0, MAX(billNumber)) AS officialLastBillNo FROM billing_Official_data bod CROSS JOIN (SELECT COALESCE(resetDate, '04-01') AS resetDate FROM billing_firm_data WHERE firmId = '${billData.firmId}' LIMIT 1) AS frm WHERE bod.firmId = '${billData.firmId}' AND (${currentDateMD} < frm.resetDate OR (${currentDateMD} >= frm.resetDate AND DATE_FORMAT(bod.billDate, '%m-%d') >= frm.resetDate AND DATE_FORMAT(bod.billCreationDate, '%m-%d') >= frm.resetDate)) FOR UPDATE;
                                                         SELECT IF(COUNT(*) = 0, 0, MAX(billNumber)) AS complimentaryLastBillNo FROM billing_Complimentary_data bcd CROSS JOIN (SELECT COALESCE(resetDate, '04-01') AS resetDate FROM billing_firm_data WHERE firmId = '${billData.firmId}' LIMIT 1) AS frm WHERE bcd.firmId = '${billData.firmId}' AND (${currentDateMD} < frm.resetDate OR (${currentDateMD} >= frm.resetDate AND DATE_FORMAT(bcd.billDate, '%m-%d') >= frm.resetDate AND DATE_FORMAT(bcd.billCreationDate, '%m-%d') >= frm.resetDate)) FOR UPDATE;
                                                         SELECT isFixed FROM billing_DineInTable_data WHERE tableNo = '${billData.tableNo}' AND billId = '${billData.billId}'`;
                            connection.query(sql_query_chkOfficial, (err, chkExist) => {
                                if (err) {
                                    console.error("Error check official bill exist or not:", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else {
                                    const isExist = isComplimentary ? (chkExist && chkExist[1].length ? true : false) : (chkExist && chkExist[0].length ? true : false);
                                    const staticBillNumber = isComplimentary ? (chkExist && chkExist[1].length ? chkExist[1][0].billNumber : 0) : (chkExist && chkExist[0].length ? chkExist[0][0].billNumber : 0);
                                    const officialLastBillNo = chkExist && chkExist[2] ? chkExist[2][0].officialLastBillNo : 0;
                                    const complimentaryLastBillNo = chkExist && chkExist[3] ? chkExist[3][0].complimentaryLastBillNo : 0;
                                    const nextOfficialBillNo = officialLastBillNo + 1;
                                    const nextComplimentaryBillNo = complimentaryLastBillNo + 1;
                                    const isTableFixed = chkExist && chkExist[4].length ? chkExist[4][0].isFixed : true;
                                    let sql_query_getBillInfo = `SELECT
                                                                     bd.billId AS billId,
                                                                     bd.billNumber AS billNumber,
                                                                     DATE_FORMAT(bd.billDate, '%d/%m/%Y') AS billDate,
                                                                     DATE_FORMAT(bd.billCreationDate, '%h:%i %p') AS billTime,
                                                                     btd.tokenNo AS tokenNo
                                                                 FROM
                                                                     billing_data AS bd
                                                                 LEFT JOIN billing_token_data AS btd ON btd.billId = bd.billId
                                                                 WHERE bd.billId = '${billData.billId}' AND bd.billType = 'Dine In'`;
                                    connection.query(sql_query_getBillInfo, (err, billInfo) => {
                                        if (err) {
                                            console.error("Error inserting new bill number:", err);
                                            connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send('Database Error');
                                            });
                                        } else {
                                            if (billInfo && billInfo.length) {
                                                const billNumber = billInfo && billInfo[0] && billInfo[0].billNunber ? billInfo[0].billNumber : 0;
                                                const tokenNo = billInfo && billInfo[0] && billInfo[0].tokenNo ? billInfo[0].tokenNo : 0;
                                                const billDate = billInfo && billInfo[0] && billInfo[0].billDate ? billInfo[0].billDate : 0;
                                                const billTime = billInfo && billInfo[0] && billInfo[0].billTime ? billInfo[0].billTime : 0;
                                                const uid1 = new Date();
                                                const bwcId = String("bwc_" + uid1.getTime() + '_' + tokenNo);
                                                const newCustomerId = String("customer_" + uid1.getTime());
                                                const newAddressId = String("addressId_" + uid1.getTime());
                                                const bwuId = String("bwu_" + uid1.getTime());
                                                const dabId = String("dab_" + uid1.getTime());

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
                                                const values = `'${billData.billId}',
                                                                '${billData.firmId}',
                                                                '${branchId}', 
                                                                '${cashier}', 
                                                                'Offline',
                                                                'Dine In',
                                                                '${billData.billPayType}',
                                                                '${billData.discountType}',
                                                                ${billData.discountValue},
                                                                ${billData.totalDiscount},
                                                                ${billData.subTotal},
                                                                ${billData.settledAmount},
                                                                ${billData.billComment ? `'${billData.billComment}'` : null},
                                                                STR_TO_DATE('${currentDate}','%b %d %Y'),
                                                                '${billData.billStatus}'`;

                                                let updateColumnField = `cashier = '${cashier}', 
                                                                         billPayType = '${billData.billPayType}',
                                                                         discountType = '${billData.discountType}',
                                                                         discountValue = ${billData.discountValue},
                                                                         totalDiscount = ${billData.totalDiscount},
                                                                         totalAmount = ${billData.subTotal},
                                                                         settledAmount = ${billData.settledAmount},
                                                                         billComment = ${billData.billComment ? `'${billData.billComment}'` : null},
                                                                         billDate = STR_TO_DATE('${currentDate}','%b %d %Y'),
                                                                         billStatus = '${billData.billStatus}'`;

                                                let updateItemColumnField = `billPayType = '${billData.billPayType}',
                                                                             billStatus = '${billData.billStatus}'`;

                                                let sql_querry_updateBillInfo = `UPDATE billing_data SET ${updateColumnField} WHERE billId = '${billData.billId}';
                                                                                 UPDATE billing_billWiseItem_data SET ${updateItemColumnField} WHERE billId = '${billData.billId}';
                                                                                 ${!isExist && billData.isOfficial && !isComplimentary ?
                                                        `INSERT INTO billing_Official_data (billNumber, ${columnData}) VALUES(${nextOfficialBillNo}, ${values})` :
                                                        !isExist && isComplimentary ?
                                                            `INSERT INTO billing_Complimentary_data (billNumber, ${columnData}) VALUES(${nextComplimentaryBillNo}, ${values})` :
                                                            `UPDATE billing_Official_data SET ${updateColumnField} WHERE billId = '${billData.billId}'`};
                                                         UPDATE billing_Complimentary_data SET ${updateColumnField} WHERE billId = '${billData.billId}'`;
                                                console.log(!isExist && billData.isOfficial, !isExist, billData.isOfficial)
                                                connection.query(sql_querry_updateBillInfo, (err) => {
                                                    if (err) {
                                                        console.error("Error inserting new bill number:", err);
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
                                                                                        WHERE bwid.billId = '${billData.billId}'`;
                                                        connection.query(sql_query_getOldItemJson, (err, oldJson) => {
                                                            if (err) {
                                                                console.error("Error Get Old Bill Wise Item Data:", err);
                                                                connection.rollback(() => {
                                                                    connection.release();
                                                                    return res.status(500).send('Database Error');
                                                                });
                                                            } else {
                                                                const oldResult = groupItemsWithAddons(oldJson);
                                                                const json1 = Object.values(JSON.parse(JSON.stringify(oldResult)));
                                                                const json2 = Object.values(JSON.parse(JSON.stringify(billData.itemsData)));

                                                                const { added, removed, modified } = compareJson(json1, json2);

                                                                const modifiedNewJson = modified.map(({ new: newItem }) => newItem);

                                                                // Added Items

                                                                const addBillWiseItemData = [];
                                                                const addItemWiseAddonData = [];

                                                                added.forEach((item, index) => {
                                                                    let uniqueId = `iwb_${Date.now() + index}_${index}`; // Unique ID generation
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

                                                                // Remove Items
                                                                let removeJsonIds = removed.length ? removed.map((item, index) => {
                                                                    return `'${item.iwbId}'`;
                                                                }).join(',') : '';

                                                                // Update Existing Data Query

                                                                let updateQuery = modifiedNewJson.length ?
                                                                    `UPDATE billing_billWiseItem_data
                                                                    SET 
                                                                    qty = CASE iwbId ` +
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
                                                                    : `SELECT * FROM user_details WHERE userId = '0'`;

                                                                let sql_query_adjustItem = `${added.length ? `INSERT INTO billing_billWiseItem_data(iwbId, billId, branchId, itemId, qty, unit, itemPrice, price, comment, billType, billPayType, billStatus, billDate)
                                                                                            VALUES ${addBillWiseItemData};` : ''}
                                                                                            ${removed.length ? `DELETE FROM billing_billWiseItem_data WHERE iwbId IN (${removeJsonIds});
                                                                                                                UPDATE billing_itemWiseSubToken_data SET itemStatus = 'cancelled' WHERE iwbId IN (${removeJsonIds});
                                                                                                                INSERT INTO billing_modifiedKot_data(modifiedId, removedBy, iwbId, itemId, qty, unit, itemPrice, price, comment)
                                                                                                                VALUES ${addRemovedKotItem};` : ''}
                                                                                            ${modifiedNewJson.length ? `${updateQuery};` : `${updateQuery};`}`;
                                                                connection.query(sql_query_adjustItem, (err) => {
                                                                    if (err) {
                                                                        console.error("Error inserting Bill Wise Item Data:", err);
                                                                        connection.rollback(() => {
                                                                            connection.release();
                                                                            return res.status(500).send('Database Error');
                                                                        });
                                                                    } else {
                                                                        let sql_query_getFirmData = `SELECT firmId, firmName, gstNumber, firmAddress, pincode, firmMobileNo, otherMobileNo FROM billing_firm_data WHERE firmId = '${billData.firmId}';
                                                                                                     SELECT
                                                                                                        btd.tokenNo,
                                                                                                        bd.billStatus,
                                                                                                        bd.billId,
                                                                                                        bd.settledAmount,
                                                                                                        SEC_TO_TIME(
                                                                                                            TIMESTAMPDIFF(
                                                                                                                SECOND,
                                                                                                                bd.billCreationDate,
                                                                                                                NOW()
                                                                                                            )
                                                                                                        ) AS timeDifference
                                                                                                     FROM billing_token_data AS btd
                                                                                                     LEFT JOIN billing_data AS bd ON bd.billId = btd.billId
                                                                                                     WHERE btd.billType = 'Dine In' AND bd.billStatus NOT IN ('complete','Cancel') AND btd.billDate = STR_TO_DATE('${currentDate}','%b %d %Y')
                                                                                                     ORDER BY btd.tokenNo ASC;
                                                                                                     SELECT subTokenNumber FROM billing_subToken_data WHERE billId = '${billData.billId}';
                                                                                                     UPDATE billing_billWiseTableNo_data SET assignCaptain = '${billData.assignCaptain ? billData.assignCaptain : cashier}' WHERE billId = '${billData.billId}';
                                                                                                     ${addItemWiseAddonData.length ? `INSERT INTO billing_itemWiseAddon_data (iwaId, iwbId, addOnsId) VALUES ${addItemWiseAddonData.join(", ")};` : ''}
                                                                                ${['complete', 'Cancel'].includes(billData.billStatus)
                                                                                ? isTableFixed == true
                                                                                    ?
                                                                                    `UPDATE billing_DineInTable_data SET billId = null WHERE tableNo = '${billData.tableNo}' AND billId = '${billData.billId}';`
                                                                                    :
                                                                                    `DELETE FROM billing_DineInTable_data WHERE billId = '${billData.billId}' AND tableNo = '${billData.tableNo}';`
                                                                                : ''}
                                                                                    DELETE FROM billing_billWiseUpi_data WHERE billId = '${billData.billId}';
                                                                                ${billData.billPayType == 'online' && billData.onlineId && billData.onlineId != 'other'
                                                                                ?
                                                                                `INSERT INTO billing_billWiseUpi_data(bwuId, onlineId, billId, amount, onlineDate)
                                                                                 VALUES('${bwuId}', '${billData.onlineId}', '${billData.billId}', '${billData.settledAmount}', STR_TO_DATE('${currentDate}','%b %d %Y'))`
                                                                                :
                                                                                ''}`;
                                                                        connection.query(sql_query_getFirmData, (err, firm) => {
                                                                            if (err) {
                                                                                console.error("Error inserting Bill Wise Item Data:", err);
                                                                                connection.rollback(() => {
                                                                                    connection.release();
                                                                                    return res.status(500).send('Database Error');
                                                                                });
                                                                            } else {
                                                                                const combinedItems = Object.values(billData.itemsData.reduce((acc, item) => {
                                                                                    const key = `${item.itemId}-${item.unit}`;

                                                                                    if (!acc[key]) {
                                                                                        acc[key] = { ...item };
                                                                                    } else {
                                                                                        acc[key].qty += item.qty;
                                                                                        acc[key].price += item.price;
                                                                                    }

                                                                                    return acc;
                                                                                }, {}));

                                                                                const sendJson = {
                                                                                    ...billData,
                                                                                    itemsData: combinedItems,
                                                                                    firmData: firm[0][0],
                                                                                    cashier: cashier,
                                                                                    billNo: billNumber,
                                                                                    officialBillNo: billData.isOfficial && !isComplimentary ? (!isExist ? nextOfficialBillNo : staticBillNumber) : isComplimentary ? (!isExist ? 'C' + nextComplimentaryBillNo : 'C' + staticBillNumber) : staticBillNumber || 'Not Available',
                                                                                    tokenNo: 'R' + tokenNo,
                                                                                    billDate: billDate,
                                                                                    billTime: billTime,
                                                                                    subTokens: firm && firm[2].length ? firm[2].map(item => item.subTokenNumber).sort((a, b) => a - b).join(", ") : null
                                                                                }
                                                                                const customerData = billData.customerDetails;
                                                                                if (customerData && customerData.customerId && customerData.addressId) {
                                                                                    let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                        INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                        VALUES ('${bwcId}', '${billData.billId}', '${customerData.customerId}', '${customerData.addressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                    connection.query(sql_query_addAddressRelation, (err) => {
                                                                                        if (err) {
                                                                                            console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                    return res.status(200).send(sendJson);
                                                                                                }
                                                                                            });
                                                                                        }
                                                                                    });
                                                                                } else if (customerData && customerData.customerId && customerData.address?.trim()) {
                                                                                    let sql_queries_chkOldAdd = `SELECT addressId, customerId FROM billing_customerAddress_data WHERE customerAddress = TRIM('${customerData.address}') AND customerLocality = '${customerData.locality}'`;
                                                                                    connection.query(sql_queries_chkOldAdd, (err, oldAdd) => {
                                                                                        if (err) {
                                                                                            console.error("Error inserting Customer New Address:", err);
                                                                                            connection.rollback(() => {
                                                                                                connection.release();
                                                                                                return res.status(500).send('Database Error');
                                                                                            });
                                                                                        } else {
                                                                                            if (oldAdd && oldAdd[0]) {
                                                                                                const existAddressId = oldAdd[0].addressId;
                                                                                                let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                    INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                    VALUES ('${bwcId}', '${billData.billId}', '${customerData.customerId}', '${existAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                                connection.query(sql_query_addAddressRelation, (err) => {
                                                                                                    if (err) {
                                                                                                        console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                                return res.status(200).send(sendJson);
                                                                                                            }
                                                                                                        });
                                                                                                    }
                                                                                                });
                                                                                            } else {
                                                                                                let sql_querry_addNewAddress = `INSERT INTO billing_customerAddress_data(addressId, customerId, customerAddress, customerLocality)
                                                                                                                                VALUES ('${newAddressId}', '${customerData.customerId}', TRIM('${customerData.address}'), ${customerData.locality ? `TRIM('${customerData.locality}')` : null})`;
                                                                                                connection.query(sql_querry_addNewAddress, (err) => {
                                                                                                    if (err) {
                                                                                                        console.error("Error inserting Customer New Address:", err);
                                                                                                        connection.rollback(() => {
                                                                                                            connection.release();
                                                                                                            return res.status(500).send('Database Error');
                                                                                                        });
                                                                                                    } else {
                                                                                                        let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                            INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                            VALUES ('${bwcId}', '${billData.billId}', '${customerData.customerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                                        connection.query(sql_query_addAddressRelation, (err) => {
                                                                                                            if (err) {
                                                                                                                console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                                        return res.status(200).send(sendJson);
                                                                                                                    }
                                                                                                                });
                                                                                                            }
                                                                                                        });
                                                                                                    }
                                                                                                })
                                                                                            }
                                                                                        }
                                                                                    });
                                                                                } else if (customerData && customerData.customerId) {
                                                                                    let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                        INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                        VALUES ('${bwcId}', '${billData.billId}', '${customerData.customerId}', ${customerData.addressId ? `'${customerData.addressId}'` : null}, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                    connection.query(sql_query_addAddressRelation, (err) => {
                                                                                        if (err) {
                                                                                            console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                    return res.status(200).send(sendJson);
                                                                                                }
                                                                                            });
                                                                                        }
                                                                                    });
                                                                                } else {
                                                                                    if (customerData && (customerData.customerName || customerData.mobileNo)) {
                                                                                        let sql_querry_getExistCustomer = `SELECT customerId, customerMobileNumber FROM billing_customer_data WHERE customerMobileNumber = '${customerData.mobileNo}'`;
                                                                                        connection.query(sql_querry_getExistCustomer, (err, num) => {
                                                                                            if (err) {
                                                                                                console.error("Error Get Existing Customer Data:", err);
                                                                                                connection.rollback(() => {
                                                                                                    connection.release();
                                                                                                    return res.status(500).send('Database Error');
                                                                                                });
                                                                                            } else {
                                                                                                const existCustomerId = num && num[0] ? num[0].customerId : null;
                                                                                                if (existCustomerId && customerData.address) {
                                                                                                    let sql_queries_chkOldAdd = `SELECT addressId, customerId FROM billing_customerAddress_data WHERE customerAddress = TRIM('${customerData.address}') AND customerLocality = '${customerData.locality}'`;
                                                                                                    connection.query(sql_queries_chkOldAdd, (err, oldAdd) => {
                                                                                                        if (err) {
                                                                                                            console.error("Error inserting Customer New Address:", err);
                                                                                                            connection.rollback(() => {
                                                                                                                connection.release();
                                                                                                                return res.status(500).send('Database Error');
                                                                                                            });
                                                                                                        } else {
                                                                                                            if (oldAdd && oldAdd[0]) {
                                                                                                                const existAddressId = oldAdd[0].addressId;
                                                                                                                let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                                    INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                                    VALUES ('${bwcId}', '${billData.billId}', '${existCustomerId}', '${existAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                                                connection.query(sql_query_addAddressRelation, (err) => {
                                                                                                                    if (err) {
                                                                                                                        console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                                                return res.status(200).send(sendJson);
                                                                                                                            }
                                                                                                                        });
                                                                                                                    }
                                                                                                                });
                                                                                                            } else {
                                                                                                                let sql_querry_addNewAddress = `INSERT INTO billing_customerAddress_data(addressId, customerId, customerAddress, customerLocality)
                                                                                                                                                VALUES ('${newAddressId}', '${existCustomerId}', TRIM('${customerData.address}'), ${customerData.locality ? `TRIM('${customerData.locality}')` : null})`;
                                                                                                                connection.query(sql_querry_addNewAddress, (err) => {
                                                                                                                    if (err) {
                                                                                                                        console.error("Error inserting Customer New Address:", err);
                                                                                                                        connection.rollback(() => {
                                                                                                                            connection.release();
                                                                                                                            return res.status(500).send('Database Error');
                                                                                                                        });
                                                                                                                    } else {
                                                                                                                        let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                                            INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                                            VALUES ('${bwcId}', '${billData.billId}', '${existCustomerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                                                        connection.query(sql_query_addAddressRelation, (err) => {
                                                                                                                            if (err) {
                                                                                                                                console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                                                        return res.status(200).send(sendJson);
                                                                                                                                    }
                                                                                                                                });
                                                                                                                            }
                                                                                                                        });
                                                                                                                    }
                                                                                                                })
                                                                                                            }
                                                                                                        }
                                                                                                    })
                                                                                                } else if (customerData.address?.trim()) {
                                                                                                    let sql_querry_addNewCustomer = `INSERT INTO billing_customer_data(customerId, customerName, customerMobileNumber, birthDate, anniversaryDate)
                                                                                                                                     VALUES ('${newCustomerId}', ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.mobileNo ? `'${customerData.mobileNo}'` : null}, ${customerData.birthDate ? `STR_TO_DATE('${customerData.birthDate}','%b %d %Y')` : null}, ${customerData.aniversaryDate ? `STR_TO_DATE('${customerData.aniversaryDate}','%b %d %Y')` : null})`;
                                                                                                    connection.query(sql_querry_addNewCustomer, (err) => {
                                                                                                        if (err) {
                                                                                                            console.error("Error inserting New Customer Data:", err);
                                                                                                            connection.rollback(() => {
                                                                                                                connection.release();
                                                                                                                return res.status(500).send('Database Error');
                                                                                                            });
                                                                                                        } else {
                                                                                                            let sql_querry_addNewAddress = `INSERT INTO billing_customerAddress_data(addressId, customerId, customerAddress, customerLocality)
                                                                                                                                            VALUES ('${newAddressId}', '${newCustomerId}', TRIM('${customerData.address}'), ${customerData.locality ? `TRIM('${customerData.locality}')` : null})`;
                                                                                                            connection.query(sql_querry_addNewAddress, (err) => {
                                                                                                                if (err) {
                                                                                                                    console.error("Error inserting Customer New Address:", err);
                                                                                                                    connection.rollback(() => {
                                                                                                                        connection.release();
                                                                                                                        return res.status(500).send('Database Error');
                                                                                                                    });
                                                                                                                } else {
                                                                                                                    let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                                        INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                                        VALUES ('${bwcId}', '${billData.billId}', '${newCustomerId}', '${newAddressId}', ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                                                    connection.query(sql_query_addAddressRelation, (err) => {
                                                                                                                        if (err) {
                                                                                                                            console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                                                    return res.status(200).send(sendJson);
                                                                                                                                }
                                                                                                                            });
                                                                                                                        }
                                                                                                                    });
                                                                                                                }
                                                                                                            })
                                                                                                        }
                                                                                                    })
                                                                                                } else if (existCustomerId) {
                                                                                                    let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                        INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                        VALUES ('${bwcId}', '${billData.billId}', '${existCustomerId}', NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                                    connection.query(sql_query_addAddressRelation, (err) => {
                                                                                                        if (err) {
                                                                                                            console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                                    return res.status(200).send(sendJson);
                                                                                                                }
                                                                                                            });
                                                                                                        }
                                                                                                    });
                                                                                                } else if (customerData.mobileNo) {
                                                                                                    let sql_querry_addNewCustomer = `INSERT INTO billing_customer_data(customerId, customerName, customerMobileNumber, birthDate, anniversaryDate)
                                                                                                                                     VALUES ('${newCustomerId}', ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.mobileNo ? `'${customerData.mobileNo}'` : null}, ${customerData.birthDate ? `STR_TO_DATE('${customerData.birthDate}','%b %d %Y')` : null}, ${customerData.aniversaryDate ? `STR_TO_DATE('${customerData.aniversaryDate}','%b %d %Y')` : null})`;
                                                                                                    connection.query(sql_querry_addNewCustomer, (err) => {
                                                                                                        if (err) {
                                                                                                            console.error("Error inserting New Customer Data:", err);
                                                                                                            connection.rollback(() => {
                                                                                                                connection.release();
                                                                                                                return res.status(500).send('Database Error');
                                                                                                            });
                                                                                                        } else {
                                                                                                            let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                                INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                                VALUES ('${bwcId}', '${billData.billId}', '${newCustomerId}', NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                                            connection.query(sql_query_addAddressRelation, (err) => {
                                                                                                                if (err) {
                                                                                                                    console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                                            return res.status(200).send(sendJson);
                                                                                                                        }
                                                                                                                    });
                                                                                                                }
                                                                                                            });
                                                                                                        }
                                                                                                    })
                                                                                                } else {
                                                                                                    let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                                        INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                                        VALUES ('${bwcId}', '${billData.billId}', NULL, NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                                    connection.query(sql_query_addAddressRelation, (err) => {
                                                                                                        if (err) {
                                                                                                            console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                                    return res.status(200).send(sendJson);
                                                                                                                }
                                                                                                            });
                                                                                                        }
                                                                                                    });
                                                                                                }
                                                                                            }
                                                                                        })
                                                                                    } else if (customerData.address?.trim() || customerData.locality?.trim()) {
                                                                                        let sql_query_addAddressRelation = `DELETE FROM billing_billWiseCustomer_data WHERE billId = '${billData.billId}';
                                                                                                                            INSERT INTO billing_billWiseCustomer_data(bwcId, billId, customerId, addressId, mobileNo, customerName, address, locality)
                                                                                                                            VALUES ('${bwcId}', '${billData.billId}', NULL, NULL, ${customerData.mobileNo ? `TRIM('${customerData.mobileNo}')` : null}, ${customerData.customerName ? `TRIM('${customerData.customerName}')` : null}, ${customerData.address ? `'${customerData.address}'` : null}, ${customerData.locality ? `'${customerData.locality}'` : null})`;
                                                                                        connection.query(sql_query_addAddressRelation, (err) => {
                                                                                            if (err) {
                                                                                                console.error("Error inserting Customer Bill Wise Data:", err);
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
                                                                                                        return res.status(200).send(sendJson);
                                                                                                    }
                                                                                                });
                                                                                            }
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
                                                                                                return res.status(200).send(sendJson);
                                                                                            }
                                                                                        });
                                                                                    }
                                                                                }
                                                                            }
                                                                        });
                                                                    }
                                                                });
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

// Sattled Bill Data After Print

const sattledBillDataByID = (req, res) => {
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

                        console.log(billData);

                        const isComplimentary = billData.billPayType == 'complimentary' ? true : false;

                        if (!billData.billId || !billData.settledAmount || !billData.billPayType || !billData.billStatus || !billData.tableNo) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(404).send('Please Fill All The Fields..!');
                            })
                        } else {
                            const currentDateMD = `DATE_FORMAT(STR_TO_DATE('${currentDate}', '%b %d %Y'), '%m-%d')`;
                            let sql_query_chkOfficial = `SELECT billId, billNumber FROM billing_Official_data WHERE billId = '${billData.billId}';
                                                         SELECT billId, billNumber FROM billing_Complimentary_data WHERE billId = '${billData.billId}';
                                                         SELECT IF(COUNT(*) = 0, 0, MAX(billNumber)) AS officialLastBillNo FROM billing_Official_data bod CROSS JOIN (SELECT COALESCE(resetDate, '04-01') AS resetDate FROM billing_firm_data WHERE firmId = (SELECT firmId FROM billing_branchWiseCategory_data WHERE categoryId = 'dineIn' AND branchId = '${branchId}') LIMIT 1) AS frm WHERE bod.firmId = (SELECT firmId FROM billing_branchWiseCategory_data WHERE categoryId = 'dineIn' AND branchId = '${branchId}') AND (${currentDateMD} < frm.resetDate OR (${currentDateMD} >= frm.resetDate AND DATE_FORMAT(bod.billDate, '%m-%d') >= frm.resetDate AND DATE_FORMAT(bod.billCreationDate, '%m-%d') >= frm.resetDate)) FOR UPDATE;
                                                         SELECT IF(COUNT(*) = 0, 0, MAX(billNumber)) AS complimentaryLastBillNo FROM billing_Complimentary_data bcd CROSS JOIN (SELECT COALESCE(resetDate, '04-01') AS resetDate FROM billing_firm_data WHERE firmId = (SELECT firmId FROM billing_branchWiseCategory_data WHERE categoryId = 'dineIn' AND branchId = '${branchId}') LIMIT 1) AS frm WHERE bcd.firmId = (SELECT firmId FROM billing_branchWiseCategory_data WHERE categoryId = 'dineIn' AND branchId = '${branchId}') AND (${currentDateMD} < frm.resetDate OR (${currentDateMD} >= frm.resetDate AND DATE_FORMAT(bcd.billDate, '%m-%d') >= frm.resetDate AND DATE_FORMAT(bcd.billCreationDate, '%m-%d') >= frm.resetDate)) FOR UPDATE;
                                                         SELECT isFixed FROM billing_DineInTable_data WHERE tableNo = '${billData.tableNo}' AND billId = '${billData.billId}'`;
                            connection.query(sql_query_chkOfficial, (err, chkExist) => {
                                if (err) {
                                    console.error("Error check official bill exist or not:", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else {
                                    const isExist = isComplimentary ? (chkExist && chkExist[1].length ? true : false) : (chkExist && chkExist[0].length ? true : false);
                                    const officialLastBillNo = chkExist && chkExist[2] ? chkExist[2][0].officialLastBillNo : 0;
                                    const complimentaryLastBillNo = chkExist && chkExist[3] ? chkExist[3][0].complimentaryLastBillNo : 0;
                                    const nextOfficialBillNo = officialLastBillNo + 1;
                                    const nextComplimentaryBillNo = complimentaryLastBillNo + 1;
                                    const isTableFixed = chkExist && chkExist[4].length ? chkExist[4][0].isFixed : true;
                                    let sql_query_getBillInfo = `SELECT
                                                                     bd.billId AS billId
                                                                 FROM
                                                                     billing_data AS bd
                                                                 WHERE bd.billId = '${billData.billId}' AND bd.billType = 'Dine In'`;
                                    connection.query(sql_query_getBillInfo, (err, billInfo) => {
                                        if (err) {
                                            console.error("Error inserting new bill number:", err);
                                            connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send('Database Error');
                                            });
                                        } else {
                                            if (billInfo && billInfo.length) {
                                                const uid1 = new Date();
                                                const bwuId = String("bwu_" + uid1.getTime());
                                                const dabId = String("dab_" + uid1.getTime());

                                                let updateColumnField = `billPayType = '${billData.billPayType}',
                                                                         discountType = '${billData.discountType}',
                                                                         discountValue = ${billData.discountValue},
                                                                         totalDiscount = ${billData.totalDiscount},
                                                                         settledAmount = ${billData.settledAmount},
                                                                         billStatus = '${billData.billStatus}'`;

                                                let updateItemColumnField = `billPayType = '${billData.billPayType}',
                                                                             billStatus = '${billData.billStatus}'`;

                                                let sql_querry_updateBillInfo = `UPDATE billing_data SET ${updateColumnField} WHERE billId = '${billData.billId}';
                                                                                 UPDATE billing_billWiseItem_data SET ${updateItemColumnField} WHERE billId = '${billData.billId}';
                                                        ${!isExist && billData.isOfficial && !isComplimentary
                                                        ?
                                                        `INSERT INTO billing_Official_data(billId, billNumber, firmId, branchId, cashier, menuStatus, billType, billPayType, discountType, discountValue, totalDiscount, totalAmount, settledAmount, billComment, billDate, billStatus)
                                                         SELECT billId, ${nextOfficialBillNo}, firmId, '${branchId}', cashier, menuStatus, billType, '${billData.billPayType}', '${billData.discountType}', ${billData.discountValue}, ${billData.totalDiscount}, totalAmount, ${billData.settledAmount}, billComment, billDate, '${billData.billStatus}' FROM billing_data WHERE billId = '${billData.billId}'`
                                                        :
                                                        !isExist && isComplimentary
                                                            ?
                                                            `INSERT INTO billing_Complimentary_data(billId, billNumber, firmId, branchId, cashier, menuStatus, billType, billPayType, discountType, discountValue, totalDiscount, totalAmount, settledAmount, billComment, billDate, billStatus)
                                                         SELECT billId, ${nextComplimentaryBillNo}, firmId, '${branchId}', cashier, menuStatus, billType, '${billData.billPayType}', '${billData.discountType}', ${billData.discountValue}, ${billData.totalDiscount}, totalAmount, ${billData.settledAmount}, billComment, billDate, '${billData.billStatus}' FROM billing_data WHERE billId = '${billData.billId}'`
                                                            :
                                                            `UPDATE billing_Official_data SET ${updateColumnField} WHERE billId = '${billData.billId}'`};
                                                         UPDATE billing_Complimentary_data SET ${updateColumnField} WHERE billId = '${billData.billId}'`;

                                                connection.query(sql_querry_updateBillInfo, (err) => {
                                                    if (err) {
                                                        console.error("Error inserting new bill number:", err);
                                                        connection.rollback(() => {
                                                            connection.release();
                                                            return res.status(500).send('Database Error');
                                                        });
                                                    } else {
                                                        let sql_query_sattledData = `${['complete', 'Cancel'].includes(billData.billStatus)
                                                            ? isTableFixed == true
                                                                ?
                                                                `UPDATE billing_DineInTable_data SET billId = null WHERE tableNo = '${billData.tableNo}' AND billId = '${billData.billId}';`
                                                                :
                                                                `DELETE FROM billing_DineInTable_data WHERE billId = '${billData.billId}' AND tableNo = '${billData.tableNo}';`
                                                            : ''}
                                                                DELETE FROM billing_billWiseUpi_data WHERE billId = '${billData.billId}';
                                                                ${billData.billPayType == 'online' && billData.onlineId && billData.onlineId != 'other'
                                                                ?
                                                                `INSERT INTO billing_billWiseUpi_data(bwuId, onlineId, billId, amount, onlineDate)
                                                                 VALUES('${bwuId}', '${billData.onlineId}', '${billData.billId}', '${billData.settledAmount}', STR_TO_DATE('${currentDate}','%b %d %Y'))`
                                                                :
                                                                ''}`;
                                                        connection.query(sql_query_sattledData, (err) => {
                                                            if (err) {
                                                                console.error("Error in sattled Data:", err);
                                                                connection.rollback(() => {
                                                                    connection.release();
                                                                    return res.status(500).send('Database Error');
                                                                });
                                                            } else {
                                                                let sql_query_getBillingData = `SELECT 
                                                                                                    bd.billId AS billId, 
                                                                                                    bd.billNumber AS billNumber,
                                                                                                    COALESCE(bod.billNumber, CONCAT('C', bcd.billNumber), 'Not Available') AS officialBillNo,
                                                                                                    CASE
                                                                                                        WHEN bd.billType = 'Pick Up' THEN CONCAT('P',btd.tokenNo)
                                                                                                        WHEN bd.billType = 'Delivery' THEN CONCAT('D',btd.tokenNo)
                                                                                                        WHEN bd.billType = 'Dine In' THEN CONCAT('R',btd.tokenNo)
                                                                                                        ELSE NULL
                                                                                                    END AS tokenNo,
                                                                                                    bwu.onlineId AS onlineId,
                                                                                                    boud.holderName AS holderName,
                                                                                                    boud.upiId AS upiId,
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
                                                                                                    bwcd.billFooterNote AS footerBill,
                                                                                                    bwcd.appriciateLine AS appriciateLine
                                                                                                FROM 
                                                                                                    billing_data AS bd
                                                                                                LEFT JOIN billing_Official_data AS bod ON bod.billId = bd.billId
                                                                                                LEFT JOIN billing_Complimentary_data AS bcd ON bcd.billId = bd.billId
                                                                                                LEFT JOIN billing_token_data AS btd ON btd.billId = bd.billId
                                                                                                LEFT JOIN billing_firm_data AS bfd ON bfd.firmId = bd.firmId
                                                                                                LEFT JOIN billing_billWiseUpi_data AS bwu ON bwu.billId = bd.billId
                                                                                                LEFT JOIN billing_onlineUPI_data AS boud ON boud.onlineId = bwu.onlineId
                                                                                                LEFT JOIN billing_category_data AS bcgd ON bcgd.categoryName = bd.billType
                                                                                                LEFT JOIN billing_branchWiseCategory_data AS bwcd ON bwcd.categoryId = bcgd.categoryId
                                                                                                WHERE bd.billId = '${billData.billId}'`;
                                                                let sql_query_getBillwiseItem = `SELECT
                                                                                                     bwid.iwbId AS iwbId,
                                                                                                     bwid.itemId AS itemId,
                                                                                                     imd.itemName AS itemName,
                                                                                                     imd.itemCode AS inputCode,
                                                                                                     SUM(bwid.qty) AS qty,
                                                                                                     bwid.unit AS unit,
                                                                                                     bwid.itemPrice AS itemPrice,
                                                                                                     SUM(bwid.price) AS price,
                                                                                                     bwid.comment AS comment
                                                                                                 FROM
                                                                                                     billing_billWiseItem_data AS bwid
                                                                                                 INNER JOIN item_menuList_data AS imd ON imd.itemId = bwid.itemId
                                                                                                 WHERE bwid.billId = '${billData.billId}'
                                                                                                 GROUP BY bwid.itemId, bwid.unit`;
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
                                                                                                   WHERE iwad.iwbId IN(SELECT COALESCE(bwid.iwbId, NULL) FROM billing_billWiseItem_data AS bwid WHERE bwid.billId = '${billData.billId}')`;
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
                                                                                                 WHERE bwcd.billId = '${billData.billId}'`;
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
                                                                                                firmId = (SELECT firmId FROM billing_data WHERE billId = '${billData.billId}')`;
                                                                let sql_query_getTableData = `SELECT
                                                                                                  bwtn.tableNo AS tableNo,
                                                                                                  bwtn.areaId AS areaId,
                                                                                                  bwtn.assignCaptain AS assignCaptain,
                                                                                                  dia.areaName AS areaName,
                                                                                                  IFNULL(CONCAT(dia.prefix, ' ', bwtn.tableNo), bwtn.tableNo) AS displayTableNo
                                                                                                FROM
                                                                                                  billing_billWiseTableNo_data AS bwtn
                                                                                                LEFT JOIN billing_dineInArea_data dia ON dia.areaId = bwtn.areaId
                                                                                                WHERE billId = '${billData.billId}'`;
                                                                let sql_query_getSubTokens = `SELECT subTokenNumber FROM billing_subToken_data WHERE billId = '${billData.billId}'`;

                                                                const sql_query_getBillData = `${sql_query_getBillingData};
                                                                                               ${sql_query_getBillwiseItem};
                                                                                               ${sql_query_getFirmData};
                                                                                               ${sql_query_getItemWiseAddons};
                                                                                               ${sql_query_getCustomerInfo};
                                                                                               ${sql_query_getTableData};
                                                                                               ${sql_query_getSubTokens}`;
                                                                connection.query(sql_query_getBillData, (err, rows) => {
                                                                    if (err) {
                                                                        console.error("Error inserting new bill number:", err);
                                                                        connection.rollback(() => {
                                                                            connection.release();
                                                                            return res.status(500).send('Database Error');
                                                                        });
                                                                    } else {
                                                                        const itemsData = rows && rows[1] ? rows[1] : [];
                                                                        const addonsData = rows && rows[3] ? rows[3] : [];

                                                                        const newItemJson = itemsData.map(item => {
                                                                            const itemAddons = addonsData.filter(addon => addon.iwbId === item.iwbId);
                                                                            return {
                                                                                ...item,
                                                                                addons: Object.fromEntries(itemAddons.map(addon => [addon.addOnsId, addon])),
                                                                                addonPrice: itemAddons.reduce((sum, { price }) => sum + price, 0)
                                                                            };
                                                                        });
                                                                        const json = {
                                                                            ...rows[0][0],
                                                                            itemsData: newItemJson,
                                                                            firmData: rows && rows[2] ? rows[2][0] : [],
                                                                            ...({ customerDetails: rows && rows[4] && rows[4][0] ? rows[4][0] : '' }),
                                                                            ...(rows && rows[5] && rows[5][0] ? { tableInfo: rows[5][0] } : ''),
                                                                            subTokens: rows && rows[6] && rows[6].length ? rows[6].map(item => item.subTokenNumber).sort((a, b) => a - b).join(", ") : null,
                                                                            tableNo: rows && rows[5] && rows[5][0] ? rows[5][0].tableNo : 0,
                                                                            ...(['online'].includes(rows[0][0].billPayType) ? {
                                                                                "upiJson": {
                                                                                    "onlineId": rows[0][0].onlineId,
                                                                                    "holderName": rows[0][0].holderName,
                                                                                    "upiId": rows[0][0].upiId
                                                                                }
                                                                            } : '')
                                                                        }
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
                                                                                return res.status(200).send(json);
                                                                            }
                                                                        });
                                                                    }
                                                                })
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

// Sattled Bill Data After Print

const updateBillDataWithPrintByID = (req, res) => {
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
                        console.log(billData);
                        const isComplimentary = billData.billPayType == 'complimentary' ? true : false;

                        if (!billData.billId || !billData.settledAmount || !billData.billPayType || !billData.billStatus || !billData.tableNo) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(404).send('Please Fill All The Fields..!');
                            })
                        } else {
                            const currentDateMD = `DATE_FORMAT(STR_TO_DATE('${currentDate}', '%b %d %Y'), '%m-%d')`;
                            let sql_query_chkOfficial = `SELECT billId, billNumber FROM billing_Official_data WHERE billId = '${billData.billId}';
                                                         SELECT billId, billNumber FROM billing_Complimentary_data WHERE billId = '${billData.billId}';
                                                         SELECT IF(COUNT(*) = 0, 0, MAX(billNumber)) AS officialLastBillNo FROM billing_Official_data bod CROSS JOIN (SELECT COALESCE(resetDate, '04-01') AS resetDate FROM billing_firm_data WHERE firmId = (SELECT firmId FROM billing_branchWiseCategory_data WHERE categoryId = 'dineIn' AND branchId = '${branchId}') LIMIT 1) AS frm WHERE bod.firmId = (SELECT firmId FROM billing_branchWiseCategory_data WHERE categoryId = 'dineIn' AND branchId = '${branchId}') AND (${currentDateMD} < frm.resetDate OR (${currentDateMD} >= frm.resetDate AND DATE_FORMAT(bod.billDate, '%m-%d') >= frm.resetDate AND DATE_FORMAT(bod.billCreationDate, '%m-%d') >= frm.resetDate)) FOR UPDATE;
                                                         SELECT IF(COUNT(*) = 0, 0, MAX(billNumber)) AS complimentaryLastBillNo FROM billing_Complimentary_data bcd CROSS JOIN (SELECT COALESCE(resetDate, '04-01') AS resetDate FROM billing_firm_data WHERE firmId = (SELECT firmId FROM billing_branchWiseCategory_data WHERE categoryId = 'dineIn' AND branchId = '${branchId}') LIMIT 1) AS frm WHERE bcd.firmId = (SELECT firmId FROM billing_branchWiseCategory_data WHERE categoryId = 'dineIn' AND branchId = '${branchId}') AND (${currentDateMD} < frm.resetDate OR (${currentDateMD} >= frm.resetDate AND DATE_FORMAT(bcd.billDate, '%m-%d') >= frm.resetDate AND DATE_FORMAT(bcd.billCreationDate, '%m-%d') >= frm.resetDate)) FOR UPDATE`;
                            connection.query(sql_query_chkOfficial, (err, chkExist) => {
                                if (err) {
                                    console.error("Error check official bill exist or not:", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else {
                                    const isExist = isComplimentary ? (chkExist && chkExist[1].length ? true : false) : (chkExist && chkExist[0].length ? true : false);
                                    const officialLastBillNo = chkExist && chkExist[2] ? chkExist[2][0].officialLastBillNo : 0;
                                    const complimentaryLastBillNo = chkExist && chkExist[3] ? chkExist[3][0].complimentaryLastBillNo : 0;
                                    const nextOfficialBillNo = officialLastBillNo + 1;
                                    const nextComplimentaryBillNo = complimentaryLastBillNo + 1;

                                    let sql_query_getBillInfo = `SELECT
                                                                     bd.billId AS billId
                                                                 FROM
                                                                     billing_data AS bd
                                                                 WHERE bd.billId = '${billData.billId}' AND bd.billType = 'Dine In'`;
                                    connection.query(sql_query_getBillInfo, (err, billInfo) => {
                                        if (err) {
                                            console.error("Error inserting new bill number:", err);
                                            connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send('Database Error');
                                            });
                                        } else {
                                            if (billInfo && billInfo.length) {
                                                const uid1 = new Date();
                                                const bwuId = String("bwu_" + uid1.getTime());
                                                const dabId = String("dab_" + uid1.getTime());

                                                let updateColumnField = `billPayType = '${billData.billPayType}',
                                                                         discountType = '${billData.discountType}',
                                                                         discountValue = ${billData.discountValue},
                                                                         totalDiscount = ${billData.totalDiscount},
                                                                         settledAmount = ${billData.settledAmount},
                                                                         billStatus = '${billData.billStatus}'`;

                                                let updateItemColumnField = `billPayType = '${billData.billPayType}',
                                                                             billStatus = '${billData.billStatus}'`;

                                                let sql_querry_updateBillInfo = `UPDATE billing_data SET ${updateColumnField} WHERE billId = '${billData.billId}';
                                                                                 UPDATE billing_billWiseItem_data SET ${updateItemColumnField} WHERE billId = '${billData.billId}';
                                                        ${!isExist && billData.isOfficial && !isComplimentary
                                                        ?
                                                        `INSERT INTO billing_Official_data(billId, billNumber, firmId, branchId, cashier, menuStatus, billType, billPayType, discountType, discountValue, totalDiscount, totalAmount, settledAmount, billComment, billDate, billStatus)
                                                         SELECT billId, ${nextOfficialBillNo}, firmId, '${branchId}', cashier, menuStatus, billType, '${billData.billPayType}', '${billData.discountType}', ${billData.discountValue}, ${billData.totalDiscount}, totalAmount, ${billData.settledAmount}, billComment, billDate, '${billData.billStatus}' FROM billing_data WHERE billId = '${billData.billId}'`
                                                        :
                                                        !isExist && isComplimentary
                                                            ?
                                                            `INSERT INTO billing_Complimentary_data(billId, billNumber, firmId, branchId, cashier, menuStatus, billType, billPayType, discountType, discountValue, totalDiscount, totalAmount, settledAmount, billComment, billDate, billStatus)
                                                         SELECT billId, ${nextComplimentaryBillNo}, firmId, '${branchId}', cashier, menuStatus, billType, '${billData.billPayType}', '${billData.discountType}', ${billData.discountValue}, ${billData.totalDiscount}, totalAmount, ${billData.settledAmount}, billComment, billDate, '${billData.billStatus}' FROM billing_data WHERE billId = '${billData.billId}'`
                                                            :
                                                            `UPDATE billing_Official_data SET ${updateColumnField} WHERE billId = '${billData.billId}'`};
                                                         UPDATE billing_Complimentary_data SET ${updateColumnField} WHERE billId = '${billData.billId}'`;

                                                connection.query(sql_querry_updateBillInfo, (err) => {
                                                    if (err) {
                                                        console.error("Error inserting new bill number:", err);
                                                        connection.rollback(() => {
                                                            connection.release();
                                                            return res.status(500).send('Database Error');
                                                        });
                                                    } else {
                                                        let sql_query_sattledData = `DELETE FROM billing_billWiseUpi_data WHERE billId = '${billData.billId}';
                                                                ${billData.billPayType == 'online' && billData.onlineId && billData.onlineId != 'other'
                                                                ?
                                                                `INSERT INTO billing_billWiseUpi_data(bwuId, onlineId, billId, amount, onlineDate)
                                                                 VALUES('${bwuId}', '${billData.onlineId}', '${billData.billId}', '${billData.settledAmount}', STR_TO_DATE('${currentDate}','%b %d %Y'))`
                                                                :
                                                                ''}`;
                                                        connection.query(sql_query_sattledData, (err) => {
                                                            if (err) {
                                                                console.error("Error in sattled Data:", err);
                                                                connection.rollback(() => {
                                                                    connection.release();
                                                                    return res.status(500).send('Database Error');
                                                                });
                                                            } else {
                                                                let sql_query_getBillingData = `SELECT 
                                                                                                    bd.billId AS billId, 
                                                                                                    bd.billNumber AS billNumber,
                                                                                                    COALESCE(bod.billNumber, CONCAT('C', bcd.billNumber), 'Not Available') AS officialBillNo,
                                                                                                    CASE
                                                                                                        WHEN bd.billType = 'Pick Up' THEN CONCAT('P',btd.tokenNo)
                                                                                                        WHEN bd.billType = 'Delivery' THEN CONCAT('D',btd.tokenNo)
                                                                                                        WHEN bd.billType = 'Dine In' THEN CONCAT('R',btd.tokenNo)
                                                                                                        ELSE NULL
                                                                                                    END AS tokenNo,
                                                                                                    bwu.onlineId AS onlineId,
                                                                                                    boud.holderName AS holderName,
                                                                                                    boud.upiId AS upiId,
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
                                                                                                    bwcd.billFooterNote AS footerBill,
                                                                                                    bwcd.appriciateLine AS appriciateLine
                                                                                                FROM 
                                                                                                    billing_data AS bd
                                                                                                LEFT JOIN billing_Official_data AS bod ON bod.billId = bd.billId
                                                                                                LEFT JOIN billing_Complimentary_data AS bcd ON bcd.billId = bd.billId
                                                                                                LEFT JOIN billing_token_data AS btd ON btd.billId = bd.billId
                                                                                                LEFT JOIN billing_firm_data AS bfd ON bfd.firmId = bd.firmId
                                                                                                LEFT JOIN billing_billWiseUpi_data AS bwu ON bwu.billId = bd.billId
                                                                                                LEFT JOIN billing_onlineUPI_data AS boud ON boud.onlineId = bwu.onlineId
                                                                                                LEFT JOIN billing_category_data AS bcgd ON bcgd.categoryName = bd.billType
                                                                                                LEFT JOIN billing_branchWiseCategory_data AS bwcd ON bwcd.categoryId = bcgd.categoryId
                                                                                                WHERE bd.billId = '${billData.billId}'`;
                                                                let sql_query_getBillwiseItem = `SELECT
                                                                                                     bwid.iwbId AS iwbId,
                                                                                                     bwid.itemId AS itemId,
                                                                                                     imd.itemName AS itemName,
                                                                                                     imd.itemCode AS inputCode,
                                                                                                     SUM(bwid.qty) AS qty,
                                                                                                     bwid.unit AS unit,
                                                                                                     bwid.itemPrice AS itemPrice,
                                                                                                     SUM(bwid.price) AS price,
                                                                                                     bwid.comment AS comment
                                                                                                 FROM
                                                                                                     billing_billWiseItem_data AS bwid
                                                                                                 INNER JOIN item_menuList_data AS imd ON imd.itemId = bwid.itemId
                                                                                                 WHERE bwid.billId = '${billData.billId}'
                                                                                                 GROUP BY bwid.itemId, bwid.unit`;
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
                                                                                                   WHERE iwad.iwbId IN(SELECT COALESCE(bwid.iwbId, NULL) FROM billing_billWiseItem_data AS bwid WHERE bwid.billId = '${billData.billId}')`;
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
                                                                                                 WHERE bwcd.billId = '${billData.billId}'`;
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
                                                                                                firmId = (SELECT firmId FROM billing_data WHERE billId = '${billData.billId}')`;
                                                                let sql_query_getTableData = `SELECT
                                                                                                  bwtn.tableNo AS tableNo,
                                                                                                  bwtn.areaId AS areaId,
                                                                                                  bwtn.assignCaptain AS assignCaptain,
                                                                                                  dia.areaName AS areaName,
                                                                                                  IFNULL(CONCAT(dia.prefix, ' ', bwtn.tableNo), bwtn.tableNo) AS displayTableNo
                                                                                                FROM
                                                                                                  billing_billWiseTableNo_data AS bwtn
                                                                                                LEFT JOIN billing_dineInArea_data dia ON dia.areaId = bwtn.areaId
                                                                                                WHERE billId = '${billData.billId}'`;
                                                                let sql_query_getSubTokens = `SELECT subTokenNumber FROM billing_subToken_data WHERE billId = '${billData.billId}'`;

                                                                const sql_query_getBillData = `${sql_query_getBillingData};
                                                                                               ${sql_query_getBillwiseItem};
                                                                                               ${sql_query_getFirmData};
                                                                                               ${sql_query_getItemWiseAddons};
                                                                                               ${sql_query_getCustomerInfo};
                                                                                               ${sql_query_getTableData};
                                                                                               ${sql_query_getSubTokens}`;
                                                                connection.query(sql_query_getBillData, (err, rows) => {
                                                                    if (err) {
                                                                        console.error("Error inserting new bill number:", err);
                                                                        connection.rollback(() => {
                                                                            connection.release();
                                                                            return res.status(500).send('Database Error');
                                                                        });
                                                                    } else {
                                                                        const itemsData = rows && rows[1] ? rows[1] : [];
                                                                        const addonsData = rows && rows[3] ? rows[3] : [];

                                                                        const newItemJson = itemsData.map(item => {
                                                                            const itemAddons = addonsData.filter(addon => addon.iwbId === item.iwbId);
                                                                            return {
                                                                                ...item,
                                                                                addons: Object.fromEntries(itemAddons.map(addon => [addon.addOnsId, addon])),
                                                                                addonPrice: itemAddons.reduce((sum, { price }) => sum + price, 0)
                                                                            };
                                                                        });
                                                                        const json = {
                                                                            ...rows[0][0],
                                                                            itemsData: newItemJson,
                                                                            firmData: rows && rows[2] ? rows[2][0] : [],
                                                                            ...({ customerDetails: rows && rows[4] && rows[4][0] ? rows[4][0] : '' }),
                                                                            ...(rows && rows[5] && rows[5][0] ? { tableInfo: rows[5][0] } : ''),
                                                                            subTokens: rows && rows[6] && rows[6].length ? rows[6].map(item => item.subTokenNumber).sort((a, b) => a - b).join(", ") : null,
                                                                            tableNo: rows && rows[5] && rows[5][0] ? rows[5][0].tableNo : 0,
                                                                            ...(['online'].includes(rows[0][0].billPayType) ? {
                                                                                "upiJson": {
                                                                                    "onlineId": rows[0][0].onlineId,
                                                                                    "holderName": rows[0][0].holderName,
                                                                                    "upiId": rows[0][0].upiId
                                                                                }
                                                                            } : '')
                                                                        }
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
                                                                                return res.status(200).send(json);
                                                                            }
                                                                        });
                                                                    }
                                                                })
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

// Move Table API

const moveTable = (req, res) => {
    try {
        const newAreaId = req.query.newAreaId ? req.query.newAreaId : null;
        const oldAreaId = req.query.oldAreaId ? req.query.oldAreaId : null;
        const tableNo = req.query.tableNo ? req.query.tableNo : null;
        const newTableNo = req.query.newTableNo ? req.query.newTableNo : null;
        const billId = req.query.billId ? req.query.billId : null;

        if (!oldAreaId || !tableNo || !newTableNo || !billId || !newAreaId) {
            return res.status(404).send('Please Fill All The Fields...!');
        } else if (newAreaId == oldAreaId && tableNo == newTableNo) {
            return res.status(400).send('Table Is Same..!');
        } else {
            let sql_query_chkExistTable = `SELECT tableNo FROM billing_DineInTable_data WHERE areaId = '${newAreaId}' AND tableNo = '${newTableNo}';
                                           SELECT isFixed FROM billing_DineInTable_data WHERE tableNo = '${tableNo}' AND billId = '${billId}' AND areaId = '${newAreaId}'`;
            pool.query(sql_query_chkExistTable, (err, chk) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    const isTableFixed = chk && chk[1].length ? chk[1][0].isFixed : true;

                    let sql_query_adjustTable = isTableFixed == true
                        ? `UPDATE billing_DineInTable_data SET billId = NULL WHERE tableNo = '${tableNo}' AND billId = '${billId}' AND areaId = '${oldAreaId}'`
                        : `DELETE FROM billing_DineInTable_data WHERE billId = '${billId}' AND tableNo = '${tableNo}' AND areaId = '${oldAreaId}'`;

                    if (chk && chk[0].length) {
                        let sql_query_chkTableEmpty = `SELECT tableNo FROM billing_DineInTable_data WHERE areaId = '${newAreaId}' AND tableNo = '${newTableNo}' AND billId IS NULL;`;
                        pool.query(sql_query_chkTableEmpty, (err, raw) => {
                            if (err) {
                                console.error("An error occurred in SQL Queery", err);
                                return res.status(500).send('Database Error');
                            } else {
                                if (raw && raw.length) {
                                    let sql_query_updateTable = `${sql_query_adjustTable};
                                                                 UPDATE billing_DineInTable_data SET billId = '${billId}' WHERE tableNo = '${newTableNo}' AND areaId = '${newAreaId}' ;
                                                                 UPDATE billing_billWiseTableNo_data SET tableNo = '${newTableNo}', areaId = '${newAreaId}' WHERE billId = '${billId}';`;
                                    pool.query(sql_query_updateTable, (err) => {
                                        if (err) {
                                            console.error("An error occurred in SQL Queery", err);
                                            return res.status(500).send('Database Error');;
                                        } else {
                                            req?.io?.emit('updateTableView');
                                            return res.status(200).send(`Table Moved From ${tableNo} ==>> ${newTableNo}`)
                                        }
                                    })
                                } else {
                                    return res.status(400).send('Table is not empty..!');
                                }
                            }
                        })
                    } else {
                        let sql_query_addTempTable = `${sql_query_adjustTable};
                                                      UPDATE billing_billWiseTableNo_data SET tableNo = '${newTableNo}', areaId = '${newAreaId}' WHERE billId = '${billId}';
                                                      INSERT INTO billing_DineInTable_data(tableId, areaId, tableNo, billId, isFixed)
                                                      VALUES ('${newTableNo}', '${newAreaId}', '${newTableNo}', '${billId}', 0);`;
                        pool.query(sql_query_addTempTable, (err) => {
                            if (err) {
                                console.error("An error occurred in SQL Queery", err);
                                return res.status(500).send('Database Error');;
                            } else {
                                req?.io?.emit('updateTableView');
                                return res.status(200).send(`Table Moved From ${tableNo} ==>> ${newTableNo}`)
                            }
                        })
                    }
                }
            })
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Cancel Running Table

const cancelBillDataByID = (req, res) => {
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

                        const billData = req.body;
                        if (!billData.billId || !billData.billStatus || !billData.tableNo) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(404).send('Please Fill All The Fields..!');
                            })
                        } else {
                            let sql_query_chkOfficial = `SELECT billId, billNumber FROM billing_Official_data WHERE billId = '${billData.billId}';
                                                         SELECT isFixed FROM billing_DineInTable_data WHERE tableNo = '${billData.tableNo}' AND billId = '${billData.billId}'`;
                            connection.query(sql_query_chkOfficial, (err, chkExist) => {
                                if (err) {
                                    console.error("Error check official bill exist or not:", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else {
                                    const isExist = chkExist && chkExist[0].length ? true : false;
                                    const isTableFixed = chkExist && chkExist[1].length ? chkExist[1][0].isFixed : true;
                                    let sql_query_getBillInfo = `SELECT
                                                                     bd.billId AS billId
                                                                 FROM
                                                                     billing_data AS bd
                                                                 WHERE bd.billId = '${billData.billId}' AND bd.billType = 'Dine In'`;
                                    connection.query(sql_query_getBillInfo, (err, billInfo) => {
                                        if (err) {
                                            console.error("Error inserting new bill number:", err);
                                            connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send('Database Error');
                                            });
                                        } else {
                                            if (billInfo && billInfo.length) {

                                                let updateColumnField = `billPayType = '${billData.billStatus}',
                                                                         billStatus = '${billData.billStatus}'`;

                                                let sql_querry_updateBillInfo = `UPDATE billing_data SET ${updateColumnField} WHERE billId = '${billData.billId}';
                                                                                 UPDATE billing_billWiseItem_data SET ${updateColumnField} WHERE billId = '${billData.billId}';
                                                                                 UPDATE billing_Official_data SET ${updateColumnField} WHERE billId = '${billData.billId}';
                                                                                 UPDATE billing_Complimentary_data SET ${updateColumnField} WHERE billId = '${billData.billId}'`;

                                                connection.query(sql_querry_updateBillInfo, (err) => {
                                                    if (err) {
                                                        console.error("Error Update Cancle bill :", err);
                                                        connection.rollback(() => {
                                                            connection.release();
                                                            return res.status(500).send('Database Error');
                                                        });
                                                    } else {
                                                        let sql_query_sattledData = `${['Cancel'].includes(billData.billStatus)
                                                            ? isTableFixed == true
                                                                ?
                                                                `UPDATE billing_DineInTable_data SET billId = null WHERE tableNo = '${billData.tableNo}' AND billId = '${billData.billId}';`
                                                                :
                                                                `DELETE FROM billing_DineInTable_data WHERE billId = '${billData.billId}' AND tableNo = '${billData.tableNo}';`
                                                            : ''}
                                                                DELETE FROM billing_billWiseUpi_data WHERE billId = '${billData.billId}';
                                                                DELETE FROM due_billAmount_data WHERE billId = '${billData.billId}'`;
                                                        connection.query(sql_query_sattledData, (err) => {
                                                            if (err) {
                                                                console.error("Error in sattled Data:", err);
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
                                                                        return res.status(200).send('Table Bill Cancel Success');
                                                                    }
                                                                });
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
        } else {
            let sql_query_chkTableIsEmpty = `SELECT tableId, tableNo FROM billing_DineInTable_data WHERE areaId = '${areaId}' AND tableNo = '${tableNo}' AND billId IS NOT NULL`;
            pool.query(sql_query_chkTableIsEmpty, (err, table) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    if (table && table.length) {
                        return res.status(401).send(`Table No. ${tableNo} is Not Available`);
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

// Sattled Cancel Token Table

const sattledCancelTokenTable = (req, res) => {
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
                    const billId = req.query.billId;
                    let sql_query_chkIsFixed = `SELECT tableNo, isFixed FROM billing_DineInTable_data WHERE tableNo = (SELECT tableNo FROM billing_billWiseTableNo_data WHERE billId = '${billId}') AND billId = '${billId}'`;
                    connection.query(sql_query_chkIsFixed, (err, chkExist) => {
                        if (err) {
                            console.error("Error check table is fixed or not:", err);
                            connection.rollback(() => {
                                connection.release();
                                return res.status(500).send('Database Error');
                            });
                        } else {
                            const isTableFixed = chkExist && chkExist.length ? chkExist[0].isFixed : true;
                            const tableNo = chkExist && chkExist.length ? chkExist[0].tableNo : true;
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
                                        let sql_query_sattledData = isTableFixed == true
                                            ?
                                            `UPDATE billing_DineInTable_data SET billId = null WHERE tableNo = '${tableNo}' AND billId = '${billId}';`
                                            :
                                            `DELETE FROM billing_DineInTable_data WHERE billId = '${billId}' AND tableNo = '${tableNo}';`;
                                        connection.query(sql_query_sattledData, (err) => {
                                            if (err) {
                                                console.error("Error in sattled Data:", err);
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
                    })
                }
            })
        } catch (error) {
            console.error('An error occurred', error);
            connection.rollback(() => {
                connection.release();
                return res.status(500).json('Internal Server Error');
            })
        }
    })
}

// Manage Table By Area

const manageTableByArea = (req, res) => {
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
                        const branchId = decoded.id.branchId;
                        const areaId = req.body.areaId ? req.body.areaId : null;
                        const num = req.body.num ? Number(req.body.num) : 0;
                        const areaName = req.body.areaName ? req.body.areaName : null;
                        const prefix = req.body.prefix ? req.body.prefix : null;
                        if (!num || !areaId || !areaName || !branchId) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(401).send('Please fill All the fields...!');
                            });
                        } else {
                            // Check if area name already exists for this branch (excluding current area)
                            let sql_query_chkAreaName = `SELECT areaId FROM billing_dineInArea_data WHERE areaName = '${areaName}' AND branchId = '${branchId}' AND areaId != '${areaId}'`;
                            connection.query(sql_query_chkAreaName, (err, chkArea) => {
                                if (err) {
                                    console.error("An error occurred in SQL Queery", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else if (chkArea && chkArea.length) {
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(400).send('name is already exist');
                                    });
                                } else {
                                    let sql_query_updateArea = `UPDATE billing_dineInArea_data SET areaName = '${areaName}', prefix = ${prefix ? `'${prefix}'` : null} WHERE areaId = '${areaId}'`;
                                    connection.query(sql_query_updateArea, (err, updateResult) => {
                                        if (err) {
                                            console.error("An error occurred in SQL Queery", err);
                                            connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send('Database Error');
                                            });
                                        } else {
                                            let sql_query_chkTableActivity = `SELECT tableNo FROM billing_DineInTable_data WHERE areaId = '${areaId}' AND billId IS NOT NULL`;
                                            connection.query(sql_query_chkTableActivity, (err, chk) => {
                                                if (err) {
                                                    console.error("An error occurred in SQL Queery", err);
                                                    connection.rollback(() => {
                                                        connection.release();
                                                        return res.status(500).send('Database Error');
                                                    });
                                                } else if (chk && chk.length) {
                                                    connection.rollback(() => {
                                                        connection.release();
                                                        return res.status(401).send('You cannot modify the data at this time...!');
                                                    });
                                                } else {
                                                    let sql_query_getTotalNumOfTable = `SELECT tableNo FROM billing_DineInTable_data WHERE areaId = '${areaId}'`;
                                                    connection.query(sql_query_getTotalNumOfTable, (err, tbl) => {
                                                        if (err) {
                                                            console.error("An error occurred in SQL Queery", err);
                                                            connection.rollback(() => {
                                                                connection.release();
                                                                return res.status(500).send('Database Error');
                                                            });
                                                        } else {
                                                            const totalTable = tbl && tbl.length ? tbl.length : 0;
                                                            const oldTableArray = tbl && tbl.length ? tbl.map((e) => Number(e.tableNo)) : [];
                                                            let array = [...Array(num).keys()].map(i => i + 1);

                                                            if (num > totalTable) {
                                                                let newArray = array.filter(item => !oldTableArray.includes(item));
                                                                console.log(newArray);
                                                                let result = newArray.map((item, index) => {
                                                                    const tableId = `table_${Date.now()}_${Math.floor(Math.random() * 10000) + "_" + index}`;
                                                                    return `('${tableId}','${areaId}','${item}',NULL,1)`;
                                                                });
                                                                let sql_query_addNewTable = `INSERT INTO billing_DineInTable_data(tableId, areaId, tableNo, billId, isFixed)
                                                                                             VALUES ${result}`;
                                                                connection.query(sql_query_addNewTable, (err, tbl) => {
                                                                    if (err) {
                                                                        console.error("An error occurred in SQL Queery", err);
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
                                                                                return res.status(201).send('Tables have been created.');
                                                                            }
                                                                        });
                                                                    }
                                                                });
                                                            } else if (num == totalTable) {
                                                                connection.commit((err) => {
                                                                    if (err) {
                                                                        console.error("Error committing transaction:", err);
                                                                        connection.rollback(() => {
                                                                            connection.release();
                                                                            return res.status(500).send('Database Error');
                                                                        });
                                                                    } else {
                                                                        connection.release();
                                                                        return res.status(200).send('Tables have been Updated.');
                                                                    }
                                                                });
                                                            } else {
                                                                let newArray = oldTableArray.filter(item => !array.includes(item));
                                                                let result = `('${newArray.join("','")}')`;
                                                                let sql_query_removeNewTable = `DELETE FROM billing_DineInTable_data WHERE areaId = '${areaId}' AND tableNo IN ${result}`;
                                                                connection.query(sql_query_removeNewTable, (err, tbl) => {
                                                                    if (err) {
                                                                        console.error("An error occurred in SQL Queery", err);
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
                                                                                return res.status(200).send('Tables have been Deleted.');
                                                                            }
                                                                        });
                                                                    }
                                                                });
                                                            }
                                                        }
                                                    });
                                                }
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    } else {
                        connection.rollback(() => {
                            connection.release();
                            return res.status(404).send('Please Login First...!');
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

// Add Area Details

const addAreaDetails = (req, res) => {
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
                        const branchId = decoded.id.branchId;
                        const areaName = req.body.areaName ? req.body.areaName : null;
                        const prefix = req.body.prefix ? req.body.prefix : null;
                        const displayRank = req.body.displayRank ? Number(req.body.displayRank) : null;
                        const num = req.body.num ? Number(req.body.num) : 0;

                        if (!branchId || !areaName || !displayRank || !num) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(401).send('Please fill All the fields...!');
                            });
                        } else {
                            // Check if area name already exists for this branch
                            let sql_query_chkAreaName = `SELECT areaId FROM billing_dineInArea_data WHERE areaName = '${areaName}' AND branchId = '${branchId}'`;
                            connection.query(sql_query_chkAreaName, (err, chkArea) => {
                                if (err) {
                                    console.error("An error occurred in SQL Queery", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send('Database Error');
                                    });
                                } else if (chkArea && chkArea.length) {
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(400).send('name is already exist');
                                    });
                                } else {
                                    const uid1 = new Date();
                                    const areaId = String("area_" + uid1.getTime());

                                    let sql_query_addArea = `INSERT INTO billing_dineInArea_data(areaId, branchId, areaName, prefix, displayRank)
                                                             VALUES('${areaId}', '${branchId}', '${areaName}', ${prefix ? `'${prefix}'` : null}, ${displayRank ? displayRank : 0})`;
                                    connection.query(sql_query_addArea, (err, areaResult) => {
                                        if (err) {
                                            console.error("An error occurred in SQL Queery", err);
                                            connection.rollback(() => {
                                                connection.release();
                                                return res.status(500).send('Database Error');
                                            });
                                        } else {
                                            // Create tables for the new area
                                            let array = [...Array(num).keys()].map(i => i + 1);
                                            let result = array.map((item, index) => {
                                                const tableId = `table_${Date.now()}_${Math.floor(Math.random() * 10000) + "_" + index}`;
                                                return `('${tableId}','${areaId}','${item}',NULL,1)`;
                                            });
                                            let sql_query_addNewTable = `INSERT INTO billing_DineInTable_data(tableId, areaId, tableNo, billId, isFixed)
                                                                         VALUES ${result.join(', ')}`;
                                            connection.query(sql_query_addNewTable, (err, tbl) => {
                                                if (err) {
                                                    console.error("An error occurred in SQL Queery", err);
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
                                                            return res.status(201).send('Area and tables have been created.');
                                                        }
                                                    });
                                                }
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    } else {
                        connection.rollback(() => {
                            connection.release();
                            return res.status(404).send('Please Login First...!');
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

// get Area Details By Id

const getAreaDeatails = (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const branchId = decoded.id.branchId;
            if (!branchId) {
                return res.status(404).send('Branch Id Not Found...!');
            } else {
                let sql_query_getAreaDetails = `SELECT 
                                                    dia.areaId, 
                                                    dia.areaName, 
                                                    dia.prefix, 
                                                    dia.displayRank,
                                                    COUNT(dt.tableId) AS tableCount
                                                FROM 
                                                    billing_dineInArea_data AS dia
                                                LEFT JOIN billing_DineInTable_data AS dt ON dt.areaId = dia.areaId
                                                WHERE dia.branchId = '${branchId}'
                                                GROUP BY dia.areaId, dia.areaName, dia.prefix, dia.displayRank
                                                ORDER BY dia.displayRank`;
                pool.query(sql_query_getAreaDetails, (err, area) => {
                    if (err) {
                        console.error("An error occurred in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    } else {
                        return res.status(200).json(area);
                    }
                });
            }
        } else {
            return res.status(404).send('Please Login First...!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Reorder Area Details

const updateDisplayRankForArea = (req, res) => {
    try {
        const jsonData = req.body ? req.body : [];
        if (jsonData && jsonData.length) {
            const sql_query_updateRank = `UPDATE billing_dineInArea_data
                                          SET displayRank = CASE areaId
                                          ${jsonData.map(item => `WHEN '${item.areaId}' THEN ${item.displayRank}`).join('\n')}
                                          END
                                          WHERE areaId IN (${jsonData.map(item => `'${item.areaId}'`).join(', ')})`;
            pool.query(sql_query_updateRank, (err, data) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    return res.status(200).send("Success");
                }
            })
        } else {
            return res.status(400).send('Please Fill All The Fields...!')
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Remove Dine In Area by ID

const removeAreaDetailsById = (req, res) => {
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
                    const areaId = req.query.areaId ? req.query.areaId : null;
                    if (!areaId) {
                        connection.rollback(() => {
                            connection.release();
                            return res.status(400).send('Please Fill All The Fields...!');
                        });
                    } else {
                        // Check if any tables have non-null billId
                        let sql_query_chkTableActivity = `SELECT billId FROM billing_DineInTable_data WHERE areaId = '${areaId}' AND billId IS NOT NULL`;
                        connection.query(sql_query_chkTableActivity, (err, chk) => {
                            if (err) {
                                console.error("An error occurred in SQL Queery", err);
                                connection.rollback(() => {
                                    connection.release();
                                    return res.status(500).send('Database Error');
                                });
                            } else if (chk && chk.length) {
                                connection.rollback(() => {
                                    connection.release();
                                    return res.status(400).send('You cannot delete this area because some tables are currently in use...!');
                                });
                            } else {
                                // Delete all tables associated with this area
                                let sql_query_deleteTables = `DELETE FROM billing_DineInTable_data WHERE areaId = '${areaId}'`;
                                connection.query(sql_query_deleteTables, (err, deleteTables) => {
                                    if (err) {
                                        console.error("An error occurred in SQL Queery", err);
                                        connection.rollback(() => {
                                            connection.release();
                                            return res.status(500).send('Database Error');
                                        });
                                    } else {
                                        // Delete the area
                                        let sql_query_removeArea = `DELETE FROM billing_dineInArea_data WHERE areaId = '${areaId}'`;
                                        connection.query(sql_query_removeArea, (err, data) => {
                                            if (err) {
                                                console.error("An error occurred in SQL Queery", err);
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
                                                        return res.status(200).send('Area has been Deleted.');
                                                    }
                                                });
                                            }
                                        });
                                    }
                                });
                            }
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

module.exports = {
    getSubTokensByBillId,
    addDineInOrder,
    removeSubTokenDataById,
    updateSubTokenDataById,
    getAllTableView,
    printTableBill,
    updateDineInBillData,
    sattledBillDataByID,
    moveTable,
    cancelBillDataByID,
    isTableEmpty,
    sattledCancelTokenTable,
    manageTableByArea,
    updateBillDataWithPrintByID,
    getAreaDeatails,
    updateDisplayRankForArea,
    addAreaDetails,
    removeAreaDetailsById
}