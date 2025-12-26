const pool = require('../../database');
const pool2 = require('../../databasePool');
const jwt = require("jsonwebtoken");
const { generateUpdateQuery, varientDatas } = require('./menuFunction.controller');
const { jsPDF } = require('jspdf');
require('jspdf-autotable');

async function createPDF(res, datas) {
    try {
        const doc = new jsPDF();

        function addSection(doc, title, items, isFirstPage = false, footer) {
            if (!isFirstPage) {
                doc.addPage();
            }
            doc.text(title, 14, 20);

            const tableData = items.map((item, index) => (
                [index + 1, item.itemName, item.soldQty, parseFloat(item.soldRevenue).toLocaleString('en-IN'), item.complimentaryQty, parseFloat(item.complimentaryRevenue).toLocaleString('en-IN'), item.cancelQty, parseFloat(item.cancelRevenue).toLocaleString('en-IN')]
            ));
            tableData.push(footer);


            const head = [
                ['Item Info', '', 'Regular', '', 'Complimentary', '', 'Cancel', ''],
                ['Sr.', 'Item Name', 'Qty', 'Revenue', 'Qty', 'Revenue', 'Qty', 'Revenue']
            ];

            doc.autoTable({
                head: head,
                body: tableData,
                startY: 30,
                theme: 'grid',
                styles: {
                    cellPadding: 2,
                    halign: 'center',
                    fontSize: 10,
                    lineWidth: 0.1, // Add border width
                    lineColor: [192, 192, 192] // Add border color
                },
                headStyles: {
                    lineWidth: 0.1, // Add border width
                    lineColor: [192, 192, 192], // Add border color
                    fontSize: 10,
                    halign: 'center',
                },
                didParseCell: function (data) {
                    if (data.row.section === 'head') {
                        if (data.row.index === 0) {
                            if (data.column.index === 0) {
                                data.cell.colSpan = 2;
                            } else if (data.column.index === 2 || data.column.index === 4 || data.column.index === 6) {
                                data.cell.colSpan = 2;
                            }
                        } else if (data.row.index === 1) {
                            if (data.column.index === 1) {
                                data.cell.rowSpan = 1;
                            }
                        }
                    }
                }
            });
        }

        let isFirstPage = true;
        Object.keys(datas).forEach((key, index) => {
            const section = datas[key];
            const footer = ['Total', '', '', parseFloat(datas[key].totalRevenue).toLocaleString('en-IN'), '', parseFloat(datas[key].totalComplimentaryRevenue).toLocaleString('en-IN'), '', parseFloat(datas[key].totalCancelRevenue).toLocaleString('en-IN')]
            addSection(doc, index + 1 + '. ' + key, section.items, isFirstPage, footer);
            isFirstPage = false;
        });

        const pdfBytes = await doc.output('arraybuffer');
        const fileName = 'jane-doe.pdf'; // Set the desired file name

        // Set the response headers for the PDF download
        res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
        res.setHeader('Content-Type', 'application/pdf');

        // Stream the PDF to the client for download
        res.send(Buffer.from(pdfBytes));

    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Get Item Data

const getItemData = (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const branchId = decoded.id.branchId;

            const menuId = req.query.menuId ? req.query.menuId : 'base_2001';
            const subCategoryId = req.query.subCategoryId ? req.query.subCategoryId : '';
            const sql_query_staticQuery = `SELECT
                                                i.itemId,
                                                i.itemName,
                                                i.itemGujaratiName,
                                                i.itemCode,
                                                i.itemShortKey,
                                                i.itemSubCategory,
                                                i.spicyLevel,
                                                i.isJain,
                                                i.isPureJain,
                                                i.itemDescription,
                                                EXISTS(
                                                    SELECT 1 FROM item_branchWiseFavourite_data f
                                                    WHERE f.itemId = i.itemId AND f.branchId = '${branchId}'
                                                ) AS isFavourite                                            
                                            FROM 
                                            item_menuList_data i`;
            if (!menuId) {
                return res.status(404).send('menuId Not Found');
            } else if (req.query.subCategoryId) {
                sql_querry_getItem = `${sql_query_staticQuery}
                                      WHERE i.itemSubCategory = '${subCategoryId}'
                                      ORDER BY i.itemCode ASC`;
            } else {
                sql_querry_getItem = `${sql_query_staticQuery}
                                      ORDER BY i.itemCode ASC`;
            }
            pool.query(sql_querry_getItem, (err, rows) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    const datas = Object.values(JSON.parse(JSON.stringify(rows)));
                    if (datas.length) {
                        varientDatas(datas, menuId)
                            .then((data) => {
                                const combinedData = datas.map((item, index) => (
                                    {
                                        ...item,
                                        variantsList: data[index].varients,
                                        allVariantsList: data[index].allVariantsList,
                                        periods: data[index].periods,
                                        status: data[index].status
                                    }
                                ))
                                return res.status(200).send(combinedData);
                            }).catch(error => {
                                console.error('Error in processing datas :', error);
                                return res.status(500).send('Internal Error');
                            });
                    } else {
                        return res.status(400).send('No Data Found');
                    }
                }
            })
        } else {
            return res.status(404).send('Please Login First...!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Add Item Data

const addItemData = (req, res) => {
    pool2.getConnection((err, conn) => {
        if (err) {
            console.log('Connection Error', err)
            return res.status(500).send('Database Connection Error');
        }
        try {
            conn.beginTransaction((err) => {
                if (err) {
                    console.log('Error In Transaction');
                    return res.status(500).send('Transaction Error');
                } else {
                    const uid1 = new Date();
                    const itemId = String("item_" + uid1.getTime());
                    const itemData = req.body;

                    if (!itemData.itemName || !itemData.itemGujaratiName || !itemData.itemCode || !itemData.itemShortKey || !itemData.itemSubCategory || !itemData.variantsList.length) {
                        conn.rollback(() => {
                            conn.release();
                            return res.status(404).send('Please Fill All The Fields..!');
                        })
                    } else {
                        let sql_query_getOldData = `SELECT itemName FROM item_menuList_data WHERE itemName = '${itemData.itemName}';
                                                    SELECT itemGujaratiName FROM item_menuList_data WHERE itemGujaratiName = '${itemData.itemGujaratiName}';
                                                    SELECT itemCode FROM item_menuList_data WHERE itemCode = '${itemData.itemCode}';
                                                    SELECT itemShortKey FROM item_menuList_data WHERE itemShortKey = '${itemData.itemShortKey}';
                                                    SELECT menuCategoryId FROM item_menuCategory_data`
                        conn.query(sql_query_getOldData, (err, oldDatas) => {
                            if (err) {
                                console.error("An error occurred in SQL Queery", err);
                                conn.rollback(() => {
                                    console.error("An error occurred in SQL Queery 1", err);
                                    conn.release();
                                    return res.status(500).send('Database Error');
                                })
                            } else {
                                const oldData = Object.values(JSON.parse(JSON.stringify(oldDatas)));
                                const menuCategoryList = oldData[4];
                                if (oldData && oldData[0].length > 0) {
                                    conn.rollback(() => {
                                        conn.release();
                                        return res.status(400).send('Item Name is Already In Use');
                                    })
                                } else if (oldData && oldData[1].length > 0) {
                                    conn.rollback(() => {
                                        conn.release();
                                        return res.status(400).send('Item GujaratiName is Already In Use');
                                    })
                                } else if (oldData && oldData[2].length > 0) {
                                    conn.rollback(() => {
                                        conn.release();
                                        return res.status(400).send('Code is Already In Use');
                                    })
                                } else if (oldData && oldData[3].length > 0) {
                                    conn.rollback(() => {
                                        conn.release();
                                        return res.status(400).send('Short Key is Already In Use');
                                    })
                                } else if (!oldData || !oldData[4] || oldData[4].length < 1) {
                                    conn.rollback(() => {
                                        conn.release();
                                        return res.status(400).send('Menu Category Not Found');
                                    })
                                } else {
                                    let sql_querry_addItem = `INSERT INTO item_menuList_data (itemId, itemName, itemGujaratiName, itemCode, itemShortKey, itemSubCategory, spicyLevel, isJain, isPureJain,itemDescription)
                                                              VALUES ('${itemId}', TRIM('${itemData.itemName}'), TRIM('${itemData.itemGujaratiName}'), ${itemData.itemCode} ,TRIM('${itemData.itemShortKey}'), '${itemData.itemSubCategory}', ${itemData.spicyLevel ? itemData.spicyLevel : 0}, ${itemData.isJain ? itemData.isJain : 0}, ${itemData.isPureJain ? itemData.isPureJain : 0}, TRIM(${itemData.itemDescription ? `'${itemData.itemDescription}'` : null}))`;
                                    conn.query(sql_querry_addItem, (err, menu) => {
                                        if (err) {
                                            conn.rollback(() => {
                                                console.error("An error occurred in SQL Queery", err);
                                                conn.release();
                                                return res.status(500).send('Database Error');
                                            })
                                        } else {
                                            const variantJson = itemData.variantsList;

                                            let addvariants = menuCategoryList.map((menuId, index) => {
                                                const tempData = variantJson.map((item, index) => {
                                                    return `('${menuId.menuCategoryId}', '${itemId}', ${item.preferredName ? `TRIM('${item.preferredName}')` : null}, '${item.unit}', ${item.price}, ${item.status})`;
                                                })
                                                return tempData.join(', ')
                                            })

                                            const newAddvarients = addvariants.join(', ');
                                            let sql_querry_addVariants = `INSERT INTO item_unitWisePrice_data (menuCategoryId, itemId, preferredName, unit, price, status)
                                                                          VALUES ${newAddvarients}`;
                                            conn.query(sql_querry_addVariants, (err, variant) => {
                                                if (err) {
                                                    conn.rollback(() => {
                                                        console.error("An error occurred in SQL Queery", err);
                                                        conn.release();
                                                        return res.status(500).send('Database Error');
                                                    })
                                                }
                                                else {
                                                    conn.commit((err) => {
                                                        if (err) {
                                                            conn.rollback(() => {
                                                                console.error("An error occurred in SQL Queery", err);
                                                                conn.release();
                                                                return res.status(500).send('Database Error');
                                                            })
                                                        } else {
                                                            conn.release();
                                                            return res.status(200).send("Item Added Successfully");
                                                        }
                                                    })
                                                }
                                            })
                                        }
                                    })
                                }
                            }
                        })
                    }
                }
            })
        } catch (error) {
            conn.rollback(() => {
                console.error('An error occurred', error);
                conn.release();
                return res.status(500).json('Internal Server Error');
            })
        }
    })
}

// Remove Item Data

const removeItemData = (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const rights = decoded.id.rights;
            if (rights == 1) {
                const itemId = req.query.itemId.trim();
                req.query.itemId = pool.query(`SELECT itemId FROM item_menuList_data WHERE itemId = '${itemId}'`, (err, row) => {
                    if (err) {
                        console.error("An error occurred in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    }
                    if (row && row.length) {
                        const sql_querry_removedetails = `DELETE FROM item_menuList_data WHERE itemId = '${itemId}'`;
                        pool.query(sql_querry_removedetails, (err, data) => {
                            if (err) {
                                console.error("An error occurred in SQL Queery", err);
                                return res.status(500).send('Database Error');
                            }
                            return res.status(200).send("Item Deleted Successfully");
                        })
                    } else {
                        return res.send('itemId Not Found');
                    }
                })
            } else {
                return res.status(400).send('You are Not Authorised');
            }
        } else {
            return res.status(404).send('Please Login First...!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Update Item Data

const updateItemData = (req, res) => {
    pool2.getConnection((err, conn) => {
        if (err) {
            console.log('Connection Error', err)
            return res.status(500).send('Database Connection Error');
        }
        try {
            conn.beginTransaction((err) => {
                if (err) {
                    console.log('Error In Transaction');
                    return res.status(500).send('Transaction Error');
                } else {
                    const itemData = req.body;
                    if (!itemData.itemName || !itemData.itemGujaratiName || !itemData.itemCode || !itemData.itemShortKey || !itemData.itemSubCategory || !itemData.variantsList.length || !itemData.menuCategoryId) {
                        conn.rollback(() => {
                            conn.release();
                            return res.status(404).send('Please Fill All The Fields..!');
                        })
                    } else {
                        let sql_query_getOldData = `SELECT itemName FROM item_menuList_data WHERE itemName = '${itemData.itemName}' AND itemId != '${itemData.itemId}';
                                                    SELECT itemGujaratiName FROM item_menuList_data WHERE itemGujaratiName = '${itemData.itemGujaratiName}' AND itemId != '${itemData.itemId}';
                                                    SELECT itemCode FROM item_menuList_data WHERE itemCode = '${itemData.itemCode}' AND itemId != '${itemData.itemId}';
                                                    SELECT itemShortKey FROM item_menuList_data WHERE itemShortKey = '${itemData.itemShortKey}' AND itemId != '${itemData.itemId}'`;
                        conn.query(sql_query_getOldData, (err, oldDatas) => {
                            if (err) {
                                console.error("An error occurred in SQL Queery", err);
                                conn.rollback(() => {
                                    console.error("An error occurred in SQL Queery 1", err);
                                    conn.release();
                                    return res.status(500).send('Database Error');
                                })
                            } else {
                                const oldData = Object.values(JSON.parse(JSON.stringify(oldDatas)));
                                if (oldData && oldData[0].length > 0) {
                                    conn.rollback(() => {
                                        conn.release();
                                        return res.status(400).send('Item Name is Already In Use');
                                    })
                                } else if (oldData && oldData[1].length > 0) {
                                    conn.rollback(() => {
                                        conn.release();
                                        return res.status(400).send('Item GujaratiName is Already In Use');
                                    })
                                } else if (oldData && oldData[2].length > 0) {
                                    conn.rollback(() => {
                                        conn.release();
                                        return res.status(400).send('Code is Already In Use');
                                    })
                                } else if (oldData && oldData[3].length > 0) {
                                    conn.rollback(() => {
                                        conn.release();
                                        return res.status(400).send('Short Key is Already In Use');
                                    })
                                } else {
                                    let sql_querry_updateData = `UPDATE
                                                                     item_menuList_data
                                                                 SET
                                                                     itemName = TRIM('${itemData.itemName}'),
                                                                     itemGujaratiName = TRIM('${itemData.itemGujaratiName}'),
                                                                     itemCode = ${itemData.itemCode},
                                                                     itemShortKey = TRIM('${itemData.itemShortKey}'),
                                                                     itemSubCategory = '${itemData.itemSubCategory}',
                                                                     spicyLevel = ${itemData.spicyLevel ? itemData.spicyLevel : 0},
                                                                     isJain = ${itemData.isJain ? itemData.isJain : 0},
                                                                     isPureJain = ${itemData.isPureJain ? itemData.isPureJain : 0},
                                                                     itemDescription = TRIM(${itemData.itemDescription ? `'${itemData.itemDescription}'` : null})
                                                                 WHERE itemId = '${itemData.itemId}'`;
                                    conn.query(sql_querry_updateData, (err, data) => {
                                        if (err) {
                                            conn.rollback(() => {
                                                console.error("An error occurred in SQL Queery", err);
                                                conn.release();
                                                return res.status(500).send('Database Error');
                                            })
                                        } else {
                                            let sql_querry_deleteOldVarients = `DELETE FROM item_unitWisePrice_data WHERE menuCategoryId = '${itemData.menuCategoryId}' AND itemId = '${itemData.itemId}'`;
                                            conn.query(sql_querry_deleteOldVarients, (err, data) => {
                                                if (err) {
                                                    conn.rollback(() => {
                                                        console.error("An error occurred in SQL Queery", err);
                                                        conn.release();
                                                        return res.status(500).send('Database Error');
                                                    })
                                                } else {
                                                    const variantJson = itemData.variantsList;

                                                    const addvariants = variantJson.map((item, index) => {
                                                        return `('${itemData.menuCategoryId}', '${itemData.itemId}', ${item.preferredName ? `TRIM('${item.preferredName}')` : null}, '${item.unit}', ${item.price}, ${item.status})`;
                                                    })

                                                    const newAddvarients = addvariants.join(', ');
                                                    let sql_querry_addVariants = `INSERT INTO item_unitWisePrice_data (menuCategoryId, itemId, preferredName, unit, price, status)
                                                                                  VALUES ${newAddvarients}`;
                                                    conn.query(sql_querry_addVariants, (err, variant) => {
                                                        if (err) {
                                                            conn.rollback(() => {
                                                                console.error("An error occurred in SQL Queery", err);
                                                                conn.release();
                                                                return res.status(500).send('Database Error');
                                                            })
                                                        }
                                                        else {
                                                            conn.commit((err) => {
                                                                if (err) {
                                                                    conn.rollback(() => {
                                                                        console.error("An error occurred in SQL Queery", err);
                                                                        conn.release();
                                                                        return res.status(500).send('Database Error');
                                                                    })
                                                                } else {
                                                                    conn.release();
                                                                    return res.status(200).send("Item Updated Successfully");
                                                                }
                                                            })
                                                        }
                                                    })
                                                }
                                            })
                                        }
                                    })
                                }
                            }
                        })
                    }
                }
            })
        } catch (error) {
            conn.rollback(() => {
                console.error('An error occurred', error);
                conn.release();
                return res.status(500).json('Internal Server Error');
            })
        }
    })
}

// Update Multiple Item Price

const updateMultipleItemPrice = (req, res) => {
    try {
        const priceJson = req.body;
        const newArray = priceJson.flatMap((item, index) => (
            [...item.variantsList]
        ))
        const validate = newArray.filter((item) => {
            if (item.price <= 0 || !item.uwpId) {
                return item;
            }
        })
        if (validate.length <= 0 || !validate) {
            const sql_qurey_updatedPrice = generateUpdateQuery(newArray);
            pool.query(sql_qurey_updatedPrice, (err, data) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                }
                return res.status(200).send('Price Updated Successfully');
            })
        }
        else {
            return res.status(401).send('Price can not be zero...!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Update Item Status

const updateItemStatus = (req, res) => {
    try {
        const menuId = req.query.menuId;
        const subCategoryId = req.query.subCategoryId;
        const itemId = req.query.itemId;
        const status = req.query.status;
        if (subCategoryId && menuId && !itemId) {
            sql_querry_updateStatus = `UPDATE
                                           item_unitWisePrice_data
                                       SET 
                                           status = ${status}
                                       WHERE itemId IN 
                                                    (SELECT COALESCE(itemId,null) FROM item_menuList_data WHERE itemSubCategory = '${subCategoryId}') 
                                       AND 
                                             menuCategoryId = '${menuId}'`;
        } else if (itemId && menuId && !subCategoryId) {
            sql_querry_updateStatus = `UPDATE
                                           item_unitWisePrice_data
                                       SET 
                                           status = ${status}
                                       WHERE itemId = '${itemId}' AND menuCategoryId = '${menuId}'`;
        } else {
            return res.status(404).send('Edit Field Not Foud');
        }
        pool.query(sql_querry_updateStatus, (err, data) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');
            }
            return res.status(200).send('Status Updated Successfully');
        })
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Get Item Sell Report

const getItemSalesReport = (req, res) => {
    try {
        var date = new Date(), y = date.getFullYear(), m = (date.getMonth());
        var firstDay = new Date(y, m, 1).toString().slice(4, 15);
        var lastDay = new Date(y, m + 1, 0).toString().slice(4, 15);

        const data = {
            subCategoryId: req.query.subCategoryId ? req.query.subCategoryId : null,
            billType: req.query.billType ? req.query.billType : '',
            startDate: (req.query.startDate ? req.query.startDate : '').slice(4, 15),
            endDate: (req.query.endDate ? req.query.endDate : '').slice(4, 15)
        }
        let sql_querry_getDetails = `SELECT
                                         uwi.itemId,
                                         CONCAT(item.itemName,' (',uwi.unit,')') AS itemName,
                                         item.itemSubCategory,
                                         iscd.subCategoryName,
                                         uwi.unit,
                                         SUM(CASE WHEN bbi.billStatus != 'cancel' AND bbi.billPayType != 'complimentary' THEN bbi.qty ELSE 0 END) AS soldQty,
                                         SUM(CASE WHEN bbi.billStatus != 'cancel' AND bbi.billPayType != 'complimentary' THEN bbi.price ELSE 0 END) AS soldRevenue,
                                         SUM(CASE WHEN bbi.billStatus != 'cancel' AND bbi.billPayType = 'complimentary' THEN bbi.qty ELSE 0 END) AS complimentaryQty,
                                         SUM(CASE WHEN bbi.billStatus != 'cancel' AND bbi.billPayType = 'complimentary' THEN bbi.price ELSE 0 END) AS complimentaryRevenue,
                                         SUM(CASE WHEN bbi.billStatus = 'cancel' THEN bbi.qty ELSE 0 END) AS cancelQty,
                                         SUM(CASE WHEN bbi.billStatus = 'cancel' THEN bbi.price ELSE 0 END) AS cancelRevenue
                                     FROM
                                         item_unitWisePrice_data AS uwi
                                     INNER JOIN item_menuList_data AS item ON item.itemId = uwi.itemId
                                     INNER JOIN item_subCategory_data AS iscd ON iscd.subCategoryId = item.itemSubCategory
                                     LEFT JOIN billing_billWiseItem_data AS bbi ON uwi.itemId = bbi.itemId 
                                     AND uwi.unit = bbi.unit 
                                     AND bbi.billDate BETWEEN STR_TO_DATE('${data.startDate ? data.startDate : firstDay}', '%b %d %Y') AND STR_TO_DATE('${data.endDate ? data.endDate : lastDay}', '%b %d %Y')
                                     AND bbi.billType LIKE '%` + data.billType + `%'
                                     WHERE uwi.menuCategoryId = '${process.env.BASE_MENU}' ${data.subCategoryId ? `AND iscd.subCategoryId = '${data.subCategoryId}'` : ''}
                                     GROUP BY
                                         uwi.itemId,
                                         uwi.unit,
                                         item.itemName,
                                         item.itemSubCategory
                                     ORDER BY
                                         uwi.itemId,
                                         CASE
                                            WHEN uwi.unit = 'NO' THEN 1
                                            WHEN uwi.unit = 'HP' THEN 2
                                            WHEN uwi.unit = 'KG' THEN 3
                                            ELSE 4
                                          END`;
        pool.query(sql_querry_getDetails, (err, data) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');
            } else if (!data.length) {
                return res.status(404).send('No Data Found');
            } else {
                const result = data.reduce((acc, item) => {
                    const key = item.subCategoryName;
                    if (!acc[key]) {
                        acc[key] = {
                            items: [],
                            totalQty: 0, totalRevenue: 0,
                            totalComplimentaryQty: 0, totalComplimentaryRevenue: 0,
                            totalCancelQty: 0, totalCancelRevenue: 0
                        };
                    }
                    acc[key].items.push(item);
                    if (item.soldRevenue !== null) {
                        acc[key].totalQty += item.soldQty;
                        acc[key].totalRevenue += item.soldRevenue;
                        acc[key].totalComplimentaryQty += item.complimentaryQty;
                        acc[key].totalComplimentaryRevenue += item.complimentaryRevenue;
                        acc[key].totalCancelQty += item.cancelQty;
                        acc[key].totalCancelRevenue += item.cancelRevenue;
                    }
                    return acc;
                }, {});
                return res.status(200).send(result[data[0].subCategoryName]);
            }
        })
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Get Item Sell Report

const exportPdfForItemSalesReport = (req, res) => {
    try {
        var date = new Date(), y = date.getFullYear(), m = (date.getMonth());
        var firstDay = new Date(y, m, 1).toString().slice(4, 15);
        var lastDay = new Date(y, m + 1, 0).toString().slice(4, 15);

        const data = {
            subCategoryId: req.query.subCategoryId ? req.query.subCategoryId : null,
            billType: req.query.billType ? req.query.billType : '',
            startDate: (req.query.startDate ? req.query.startDate : '').slice(4, 15),
            endDate: (req.query.endDate ? req.query.endDate : '').slice(4, 15)
        }
        let sql_querry_getDetails = `SELECT
                                         uwi.itemId,
                                         CONCAT(item.itemName,' (',uwi.unit,')') AS itemName,
                                         item.itemSubCategory,
                                         iscd.subCategoryName,
                                         uwi.unit,
                                         SUM(CASE WHEN bbi.billStatus != 'cancel' AND bbi.billPayType != 'complimentary' THEN bbi.qty ELSE 0 END) AS soldQty,
                                         SUM(CASE WHEN bbi.billStatus != 'cancel' AND bbi.billPayType != 'complimentary' THEN bbi.price ELSE 0 END) AS soldRevenue,
                                         SUM(CASE WHEN bbi.billStatus != 'cancel' AND bbi.billPayType = 'complimentary' THEN bbi.qty ELSE 0 END) AS complimentaryQty,
                                         SUM(CASE WHEN bbi.billStatus != 'cancel' AND bbi.billPayType = 'complimentary' THEN bbi.price ELSE 0 END) AS complimentaryRevenue,
                                         SUM(CASE WHEN bbi.billStatus = 'cancel' THEN bbi.qty ELSE 0 END) AS cancelQty,
                                         SUM(CASE WHEN bbi.billStatus = 'cancel' THEN bbi.price ELSE 0 END) AS cancelRevenue
                                     FROM
                                         item_unitWisePrice_data AS uwi
                                     INNER JOIN item_menuList_data AS item ON item.itemId = uwi.itemId
                                     INNER JOIN item_subCategory_data AS iscd ON iscd.subCategoryId = item.itemSubCategory
                                     LEFT JOIN billing_billWiseItem_data AS bbi ON uwi.itemId = bbi.itemId 
                                     AND uwi.unit = bbi.unit 
                                     AND bbi.billDate BETWEEN STR_TO_DATE('${data.startDate ? data.startDate : firstDay}', '%b %d %Y') AND STR_TO_DATE('${data.endDate ? data.endDate : lastDay}', '%b %d %Y')
                                     AND bbi.billType LIKE '%` + data.billType + `%'
                                     WHERE uwi.menuCategoryId = '${process.env.BASE_MENU}' ${data.subCategoryId ? `AND iscd.subCategoryId = '${data.subCategoryId}'` : ''}
                                     GROUP BY
                                         uwi.itemId,
                                         uwi.unit,
                                         item.itemName,
                                         item.itemSubCategory
                                     ORDER BY
                                         uwi.itemId,
                                         CASE
                                            WHEN uwi.unit = 'NO' THEN 1
                                            WHEN uwi.unit = 'HP' THEN 2
                                            WHEN uwi.unit = 'KG' THEN 3
                                            ELSE 4
                                          END`;
        pool.query(sql_querry_getDetails, (err, data) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');
            } else {
                const result = data.reduce((acc, item) => {
                    const key = item.subCategoryName;
                    if (!acc[key]) {
                        acc[key] = {
                            items: [],
                            totalQty: 0, totalRevenue: 0,
                            totalComplimentaryQty: 0, totalComplimentaryRevenue: 0,
                            totalCancelQty: 0, totalCancelRevenue: 0
                        };
                    }
                    acc[key].items.push(item);
                    if (item.soldRevenue !== null) {
                        acc[key].totalQty += item.soldQty;
                        acc[key].totalRevenue += item.soldRevenue;
                        acc[key].totalComplimentaryQty += item.complimentaryQty;
                        acc[key].totalComplimentaryRevenue += item.complimentaryRevenue;
                        acc[key].totalCancelQty += item.cancelQty;
                        acc[key].totalCancelRevenue += item.cancelRevenue;
                    }
                    return acc;
                }, {});
                createPDF(res, result)
                    .then(() => {
                        console.log('PDF created successfully');
                        res.status(200);
                    })
                    .catch((err) => {
                        console.log(err);
                        res.status(500).send('Error creating PDF');
                    });
            }
        })
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Update Item Price By Billing Category

const updateItemPriceByMenuId = (req, res) => {
    try {
        const itemsArray = req.body;
        const menuCategoryId = itemsArray.menuId;

        // Create an array of promises for each item
        const queries = itemsArray.itemsData.map((item) => {
            const { itemId, unit, qty, comment } = item;

            let sql_query_updatePrice = `SELECT
                                             uwp.itemId AS itemId,
                                             imd.itemName AS itemName,
                                             imd.itemCode AS inputCode,
                                             ${qty} AS qty,
                                             uwp.unit AS unit,
                                             uwp.price AS itemPrice,
                                             ${qty} * uwp.price AS price,
                                             '${comment}' AS comment,
                                             uwp.status AS itemStatus
                                         FROM
                                             item_unitWisePrice_data AS uwp
                                         INNER JOIN item_menuList_data AS imd ON imd.itemId = uwp.itemId 
                                         WHERE uwp.menuCategoryId = '${menuCategoryId}' AND uwp.itemId = '${itemId}' AND uwp.unit = '${unit}'`;

            // Return a promise for each query
            return new Promise((resolve, reject) => {
                pool.query(sql_query_updatePrice, (err, data) => {
                    if (err) {
                        return reject(err);
                    }
                    resolve(data[0]);
                });
            });
        });

        // Wait for all queries to finish
        Promise.all(queries)
            .then(results => {
                const availableItem = results.filter(item => item.itemStatus !== 0);
                const total = availableItem.reduce((acc, item) => acc + item.price, 0);
                const result = { total, itemsData: availableItem };

                return res.status(200).send(result);
            })
            .catch(error => {
                console.error('An error occured', error);
                return res.status(500).send('Database Error');
            });

    } catch (error) {
        console.error('An error occurred', error);
        return res.status(500).send('Internal Server Error');
    }
}

// Get Item Data For Touch View

const getItmeDataForTouchView = (req, res) => {
    try {
        let token;
        token = req.headers && req.headers.authorization ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const branchId = decoded.id.branchId;

            const menuId = req.query.menuId ? req.query.menuId : 'base_2001';
            const searchWord = req.query.searchWord ? req.query.searchWord : '';
            const sql_query_staticQuery = `SELECT
                                           imd.itemId AS itemId,
                                           imd.itemName AS itemName,
                                           imd.itemGujaratiName AS itemGujaratiName,
                                           imd.itemCode AS itemCode,
                                           imd.itemShortKey AS itemShortKey,
                                           imd.itemSubCategory AS itemSubCategory,
                                           iscd.subCategoryName AS subCategoryName,
                                           iscd.displayRank AS displayRank,
                                           imd.spicyLevel AS spicyLevel,
                                           imd.isJain AS isJain,
                                           imd.isPureJain AS isPureJain,
                                           imd.itemDescription AS itemDescription
                                       FROM
                                           item_menuList_data AS imd
                                       INNER JOIN item_subCategory_data AS iscd ON iscd.subCategoryId = imd.itemSubCategory`;
            let sql_querry_getItem = `${sql_query_staticQuery}
                                      WHERE imd.itemName LIKE '%` + searchWord + `%'
                                      ORDER BY iscd.displayRank ASC, imd.itemName ASC;
                                      ${sql_query_staticQuery}
                                      WHERE imd.itemId IN (SELECT COALESCE(itemId,NULL) FROM item_branchWiseFavourite_data WHERE branchId = '${branchId}')
                                      ORDER BY imd.itemName ASC;`;
            pool.query(sql_querry_getItem, (err, rows) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    const datas = Object.values(JSON.parse(JSON.stringify(rows[0])));
                    if (datas.length) {
                        varientDatas(datas, menuId)
                            .then((data) => {
                                const combinedData = datas.map((item, index) => (
                                    {
                                        ...item,
                                        variantsList: data[index].varients,
                                        allVariantsList: data[index].allVariantsList,
                                        periods: data[index].periods,
                                        status: data[index].status
                                    }
                                ))

                                const favouritesData = Object.values(JSON.parse(JSON.stringify(rows[1])));
                                varientDatas(favouritesData, menuId)
                                    .then((fdata) => {
                                        const favouriteCombinedData = favouritesData.map((item, index) => (
                                            {
                                                ...item,
                                                variantsList: fdata[index].varients,
                                                allVariantsList: fdata[index].allVariantsList,
                                                periods: fdata[index].periods,
                                                status: fdata[index].status
                                            }
                                        ))

                                        const result = combinedData.reduce((acc, item) => {
                                            const key = item.subCategoryName;
                                            if (!acc[key]) {
                                                acc[key] = [];
                                            }
                                            acc[key].push(item);
                                            return acc;
                                        }, {});
                                        let categoryArray = Object.keys(result)
                                        if (!searchWord) {
                                            favouriteCombinedData.length !== 0 ? categoryArray.unshift('Favourite Items',) : '';
                                        }

                                        const newJson = {
                                            categoryList: categoryArray,
                                            itemList: searchWord ? result : {
                                                'Favourite Items': favouriteCombinedData,
                                                ...result
                                            }
                                        }
                                        return res.status(200).send(newJson);
                                    }).catch(error => {
                                        console.error('Error in processing datas :', error);
                                        return res.status(500).send('Internal Error');
                                    });
                            }).catch(error => {
                                console.error('Error in processing datas :', error);
                                return res.status(500).send('Internal Error');
                            });
                    } else {
                        return res.status(400).send('No Data Found');
                    }
                }
            })
        } else {
            return res.status(404).send('Please Login First...!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Get Item Data By Code

const getItemDataByCode = (req, res) => {
    try {
        const menuId = req.query.menuId ? req.query.menuId : null;
        const itemCode = req.query.itemCode ? req.query.itemCode : null;
        if (!itemCode) {
            return res.status(404).send('Please Enter Code');
        } else {
            const sql_query_staticQuery = `SELECT
                                           imd.itemId AS itemId,
                                           imd.itemName AS itemName,
                                           imd.itemGujaratiName AS itemGujaratiName,
                                           imd.itemCode AS itemCode,
                                           imd.itemShortKey AS itemShortKey,
                                           imd.itemSubCategory AS itemSubCategory,
                                           iscd.subCategoryName AS subCategoryName,
                                           imd.spicyLevel AS spicyLevel,
                                           imd.isJain AS isJain,
                                           imd.isPureJain AS isPureJain,
                                           imd.itemDescription AS itemDescription
                                       FROM
                                           item_menuList_data AS imd
                                       INNER JOIN item_subCategory_data AS iscd ON iscd.subCategoryId = imd.itemSubCategory`;
            let sql_querry_getItem = `${sql_query_staticQuery}
                                      WHERE imd.itemCode = ${itemCode}`;
            pool.query(sql_querry_getItem, (err, rows) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    const datas = Object.values(JSON.parse(JSON.stringify(rows)));
                    if (datas.length) {
                        varientDatas(datas, menuId)
                            .then((data) => {
                                const combinedData = datas.map((item, index) => (
                                    {
                                        ...item,
                                        variantsList: data[index].varients,
                                        allVariantsList: data[index].allVariantsList,
                                        periods: data[index].periods,
                                        status: data[index].status
                                    }
                                ))
                                return res.status(200).send(combinedData[0]);
                            }).catch(error => {
                                console.error('Error in processing datas :', error);
                                return res.status(500).send('Internal Error');
                            });
                    } else {
                        return res.status(400).send('No Data Found');
                    }
                }
            })
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Add to Favourite By Branch

const addFavouritemByBranch = (req, res) => {
    try {
        let token;
        token = req.headers && req.headers.authorization ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const branchId = decoded.id.branchId;
            const uid1 = new Date();
            const favouriteId = String("favourite_" + uid1.getTime());
            const itemId = req.body.itemId ? req.body.itemId : null;
            const isFavourite = req.body.isFavourite ? req.body.isFavourite : false;

            if (!branchId) {
                return res.status(400).send("branchId Not Found");
            } else if (!itemId) {
                return res.status(400).send("itemId Not Found");
            } else {
                const sql_query_existData = `SELECT itemId FROM item_branchWiseFavourite_data WHERE itemId = '${itemId}' AND branchId = '${branchId}'`;
                pool.query(sql_query_existData, (err, row) => {
                    if (err) {
                        console.error("An error occurred in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    } else {
                        if ((row && !row.length) && !isFavourite) {
                            return res.status(400).send(`Item Not Exist..!`);
                        } else if ((row && row.length) && isFavourite) {
                            return res.status(400).send(`Item is Already In Favourite`);
                        } else {
                            const sql_querry_addFavourite = `INSERT INTO item_branchWiseFavourite_data(favouriteId, branchId, itemId)
                                                             VALUES ('${favouriteId}', '${branchId}', '${itemId}')`;
                            const sql_query_removeFavourite = `DELETE FROM item_branchWiseFavourite_data WHERE itemId = '${itemId}' AND branchId = '${branchId}'`;
                            pool.query(isFavourite ? sql_querry_addFavourite : sql_query_removeFavourite, (err, data) => {
                                if (err) {
                                    console.error("An error occurred in SQL Queery", err);
                                    return res.status(500).send('Database Error');
                                } else {
                                    return res.status(200).send(isFavourite ? `Item added to your Favourites!` : `Item removed to your Favourites!`);
                                }
                            })
                        }
                    }
                })
            }
        } else {
            return res.status(404).send('Please Login First...!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

module.exports = {
    getItemData,
    addItemData,
    removeItemData,
    updateItemData,
    updateMultipleItemPrice,
    updateItemStatus,
    getItemSalesReport,
    updateItemPriceByMenuId,
    exportPdfForItemSalesReport,
    getItmeDataForTouchView,
    getItemDataByCode,
    addFavouritemByBranch
}