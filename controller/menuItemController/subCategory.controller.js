const pool = require('../../database');
const pool2 = require('../../databasePool');
const jwt = require("jsonwebtoken");
const { periodDatas } = require('./menuFunction.controller')

// Get Sub Category Data

const getSubCategoryList = (req, res) => {
    try {
        const page = req.query.page;
        const numPerPage = req.query.numPerPage;
        const skip = (page - 1) * numPerPage;
        const limit = skip + ',' + numPerPage;

        var date = new Date(), y = date.getFullYear(), m = (date.getMonth());
        var firstDay = new Date(y, m, 1).toString().slice(4, 15);
        var lastDay = new Date(y, m + 1, 0).toString().slice(4, 15);
        const startDate = (req.query.startDate ? req.query.startDate : '').slice(4, 15);
        const endDate = (req.query.endDate ? req.query.endDate : '').slice(4, 15);

        sql_querry_getCountDetails = `SELECT count(*) as numRows FROM item_subCategory_data`;
        pool.query(sql_querry_getCountDetails, (err, rows, fields) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');
            } else {
                const numRows = rows[0].numRows;
                const numPages = Math.ceil(numRows / numPerPage);
                const sql_query_getDetails = `WITH FilteredBillingData AS (
                                                SELECT
                                                    itemId,
                                                    SUM(price) AS totalRs
                                                FROM
                                                    billing_billWiseItem_data
                                                WHERE
                                                    billDate BETWEEN STR_TO_DATE('${startDate ? startDate : firstDay}', '%b %d %Y') AND STR_TO_DATE('${endDate ? endDate : lastDay}', '%b %d %Y')
                                                    AND billPayType NOT IN ('Cancel', 'complimentary')
                                                    AND billStatus != 'Cancel'
                                                GROUP BY itemId
                                            )
                                            SELECT
                                                iscd.subCategoryId,
                                                iscd.subCategoryName,
                                                iscd.displayRank,
                                                COALESCE(SUM(fbd.totalRs), 0) AS totalRs
                                            FROM
                                                item_subCategory_data AS iscd
                                            LEFT JOIN item_menuList_data AS imld ON imld.itemSubCategory = iscd.subCategoryId
                                            LEFT JOIN FilteredBillingData AS fbd ON fbd.itemId = imld.itemId
                                            GROUP BY
                                                iscd.subCategoryId,
                                                iscd.subCategoryName
                                            ORDER BY
                                                iscd.subCategoryName ASC
                                                LIMIT ${limit}`;
                pool.query(sql_query_getDetails, (err, rows, fields) => {
                    if (err) {
                        console.error("An error occurred in SQL Queery", err);
                        return res.status(500).send('Database Error');;
                    } else {
                        if (numRows === 0) {
                            const rows = [{
                                'msg': 'No Data Found'
                            }]
                            return res.status(200).send({ rows, numRows });
                        } else {
                            const datas = Object.values(JSON.parse(JSON.stringify(rows)));
                            if (datas.length) {
                                periodDatas(datas)
                                    .then((data) => {
                                        const rows = datas.map((item, index) => (
                                            { ...item, periods: data[index].periods }
                                        ))
                                        return res.status(200).send({ rows, numRows });
                                    }).catch(error => {
                                        console.error('Error in processing datas :', error);
                                        return res.status(500).send('Internal Error');
                                    })
                            } else {
                                return res.status(400).send('No Data Found');
                            }
                        }
                    }
                })
            }
        })
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Get Sub Category Data

const getSubCategoryListForMobile = (req, res) => {
    try {
        const sql_query_getDetails = `SELECT subCategoryId, subCategoryName, displayRank FROM item_subCategory_data`;
        pool.query(sql_query_getDetails, (err, rows, fields) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');
            } else {
                if (!rows.length) {
                    const rows = [{
                        'msg': 'No Data Found'
                    }]
                    return res.status(200).send(rows);
                } else {
                    const datas = Object.values(JSON.parse(JSON.stringify(rows)));
                    if (datas.length) {
                        periodDatas(datas)
                            .then((data) => {
                                const rows = datas.map((item, index) => (
                                    { ...item, periods: data[index].periods }
                                ))
                                return res.status(200).send(rows);
                            }).catch(error => {
                                console.error('Error in processing datas :', error);
                                return res.status(500).send('Internal Error');
                            })
                    } else {
                        return res.status(400).send('No Data Found');
                    }
                }
            }
        })
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Get DDL For SubCategory

const ddlSubCategory = (req, res) => {
    try {
        const menuId = req.query.menuId ? req.query.menuId : null
        if (menuId) {
            sql_querry_getddlCategory = `SELECT 
                                            subCategoryId, 
                                            subCategoryName,
                                            (
                                                SELECT COUNT(*)
                                                FROM item_menuList_data imd
                                                WHERE imd.itemSubCategory = subCategoryId
                                            ) AS numberOfItem,
                                            CASE
                                                WHEN EXISTS (
                                                    SELECT 1
                                                    FROM item_unitWisePrice_data iup
                                                    JOIN item_menuList_data id ON id.itemId = iup.itemId
                                                    WHERE id.itemSubCategory = subCategoryId
                                                      AND iup.status = 1 AND iup.menuCategoryId = '${menuId}'
                                                ) THEN true
                                                ELSE false
                                            END AS status
                                         FROM item_subCategory_data
                                         ORDER BY displayRank ASC`;
        } else {
            sql_querry_getddlCategory = `SELECT 
                                            subCategoryId, 
                                            subCategoryName,
                                            (
                                                SELECT COUNT(*)
                                                FROM item_menuList_data imd
                                                WHERE imd.itemSubCategory = subCategoryId
                                            ) AS numberOfItem
                                         FROM item_subCategory_data
                                         ORDER BY displayRank ASC`;
        }

        pool.query(sql_querry_getddlCategory, (err, data) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');
            }
            return res.status(200).send(data);
        })
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Add Sub-Category Data

const addSubCategoryData = (req, res) => {
    try {
        const uid1 = new Date();
        const subCategoryId = String("subCategory_" + uid1.getTime());

        const data = {
            subCategoryName: req.body.subCategoryName.trim(),
            displayRank: req.body.displayRank
        }
        if (!data.subCategoryName || !data.displayRank) {
            return res.status(400).send("Please Fill All The Fields");
        } else {
            req.body.productName = pool.query(`SELECT subCategoryName FROM item_subCategory_data WHERE subCategoryName = '${data.subCategoryName}'`, function (err, row) {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                }
                if (row && row.length) {
                    return res.status(400).send('SubCategory is Already In Use');
                } else {
                    const sql_querry_addCategory = `INSERT INTO item_subCategory_data (subCategoryId, subCategoryName, displayRank)
                                                    VALUES ('${subCategoryId}', '${data.subCategoryName}', ${data.displayRank})`;
                    pool.query(sql_querry_addCategory, (err, data) => {
                        if (err) {
                            console.error("An error occurred in SQL Queery", err);
                            return res.status(500).send('Database Error');
                        }
                        return res.status(200).send("SubCategory Added Successfully");
                    })
                }
            })
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Remove Sub-Category Data

const removeSubCategoryData = (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const rights = decoded.id.rights;
            if (rights == 1) {
                const subCategoryId = req.query.subCategoryId.trim();
                req.query.subCategoryId = pool.query(`SELECT subCategoryId FROM item_subCategory_data WHERE subCategoryId = '${subCategoryId}'`, (err, row) => {
                    if (err) {
                        console.error("An error occurred in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    }
                    if (row && row.length) {
                        const sql_querry_removedetails = `DELETE FROM item_subCategory_data WHERE subCategoryId = '${subCategoryId}'`;
                        pool.query(sql_querry_removedetails, (err, data) => {
                            if (err) {
                                console.error("An error occurred in SQL Queery", err);
                                return res.status(500).send('Database Error');
                            }
                            return res.status(200).send("SubCategory Deleted Successfully");
                        })
                    } else {
                        return res.send('SubCategoryId Not Found');
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
        res.status(500).json('Internal Server Error');
    }
}

// Update Sub-Category Data

const updateSubCategoryData = (req, res) => {
    try {
        const data = {
            subCategoryId: req.body.subCategoryId,
            subCategoryName: req.body.subCategoryName.trim(),
            displayRank: req.body.displayRank
        }
        if (!data.subCategoryName || !data.displayRank) {
            return res.status(400).send("Please Fill All The Fields");
        }
        const sql_querry_updatedetails = `UPDATE
                                              item_subCategory_data
                                          SET
                                              subCategoryName = '${data.subCategoryName}',
                                              displayRank = ${data.displayRank}
                                          WHERE subCategoryId = '${data.subCategoryId}'`;
        pool.query(sql_querry_updatedetails, (err, data) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');
            }
            return res.status(200).send("subCategory Updated Successfully");
        })
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Add Sub Category Period Data

const addSubCategoryPeriod = (req, res) => {
    try {
        const periodData = req.body;

        if (periodData && periodData.subCategoryId && periodData.periodIntervels && periodData.periodIntervels.length > 0) {
            if (periodData.periodIntervels.length > 3) {
                res.status(400).send("Only Three intervals are allowed");
            }
            else {
                const periodJson = periodData.periodIntervels
                let addPeriodData = periodJson.map((item, index) => {
                    let uniqueId = `period_${Date.now() + index}`; // Generating a unique ID using current timestamp
                    return `('${uniqueId}', '${periodData.subCategoryId}', '${item.startTime}', '${item.endTIme}')`;
                }).join(', ');

                const sql_querry_addCategory = `INSERT INTO item_subCategoryPeriod_data (periodId, subCategoryId, startTime, endTime)
                                                VALUES ${addPeriodData}`;
                pool.query(sql_querry_addCategory, (err, data) => {
                    if (err) {
                        console.error("An error occurred in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    }
                    return res.status(200).send("Perioad Added Successfully");
                })
            }
        } else {
            res.status(400).send("Please Fill All The Fields...!");
        }

    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Update Sub Category Data

const updateSubCategoryPeriod = (req, res) => {
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
                }
                const periodData = req.body;

                if (periodData && periodData.subCategoryId) {
                    const periodJson = periodData.periodIntervels
                    sql_querry_removeOldPeriods = `DELETE FROM item_subCategoryPeriod_data WHERE subCategoryId = '${periodData.subCategoryId}'`;
                    conn.query(sql_querry_removeOldPeriods, (err, data) => {
                        if (err) {
                            conn.rollback(() => {
                                console.error("An error occurred in SQL Queery 1", err);
                                conn.release();
                                return res.status(500).send('Database Error 1');
                            })
                        } else if (periodJson.length) {
                            let addPeriodData = periodJson.map((item, index) => {
                                let uniqueId = `period_${Date.now() + index}`; // Generating a unique ID using current timestamp
                                return `('${uniqueId}', '${periodData.subCategoryId}', '${item.startTime}', '${item.endTime}')`;
                            }).join(', ');

                            const sql_querry_addCategory = `INSERT INTO item_subCategoryPeriod_data (periodId, subCategoryId, startTime, endTime)
                                                            VALUES ${addPeriodData}`;
                            conn.query(sql_querry_addCategory, (err, data) => {
                                if (err) {
                                    conn.rollback(() => {
                                        console.error("An error occurred in SQL Queery 2", err);
                                        conn.release();
                                        return res.status(500).send('Database Error');
                                    })
                                } else {
                                    conn.commit((err) => {
                                        if (err) {
                                            conn.rollback(() => {
                                                console.error("An error occurred in SQL Queery 1", err);
                                                conn.release();
                                                return res.status(500).send('Database Error');
                                            })
                                        } else {
                                            conn.release();
                                            return res.status(200).send("Perioad Update Successfully");
                                        }
                                    })
                                }
                            })
                        } else {
                            conn.commit((err) => {
                                if (err) {
                                    conn.rollback(() => {
                                        console.error("An error occurred in SQL Queery", err);
                                        conn.release();
                                        return res.status(500).send('Database Error');
                                    })
                                } else {
                                    conn.release();
                                    console.log('success;>>');
                                    return res.status(200).send("Perioad Remove Successfully");
                                }
                            })
                        }
                    })
                } else {
                    conn.rollback(() => {
                        conn.release();
                        return res.status(400).send("Please provide SubCategoryId");
                    })
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

module.exports = {
    getSubCategoryList,
    ddlSubCategory,
    addSubCategoryData,
    removeSubCategoryData,
    updateSubCategoryData,
    addSubCategoryPeriod,
    updateSubCategoryPeriod,
    getSubCategoryListForMobile
}