const pool = require('../../database');
const jwt = require("jsonwebtoken");
const pool2 = require('../../databasePool');
const baseMenuId = process.env.BASE_MENU;

// Get Manufacture Product Category List

const getMenuCategory = async (req, res) => {
    try {
        var sql_queries_getCategoryTable = `SELECT
                                                imcd.menuCategoryId,
                                                imcd.menuCategoryName
                                            FROM
                                                item_menuCategory_data AS imcd
                                            ORDER BY imcd.menuCategoryName`;

        pool.query(sql_queries_getCategoryTable, (err, rows, fields) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');;
            } else {
                return res.status(200).send(rows);
            }
        });
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Add Manufacture Product Category API

const addMenuCategory = async (req, res) => {
    pool2.getConnection((error, conn) => {
        if (error) {
            console.log('Connection Error', error)
            return res.status(500).send('Database Connection Error');
        }
        try {
            conn.beginTransaction((err) => {
                if (err) {
                    console.log('Error In Transaction', err);
                    return res.status(500).send('Transaction Error');
                }
                let token;
                token = req.headers ? req.headers.authorization.split(" ")[1] : null;
                if (token) {
                    const decoded = jwt.verify(token, process.env.JWT_SECRET);
                    const rights = decoded.id.rights;
                    if (rights == 1) {
                        const uid1 = new Date();
                        const menuCategoryId = String("menuCategory_" + uid1.getTime());

                        const data = {
                            menuCategoryName: req.body.menuCategoryName.trim(),
                        }
                        if (!data.menuCategoryName) {
                            conn.rollback(() => {
                                conn.release();
                                return res.status(400).send("Please Add Category");
                            })
                        } else {
                            req.body.productName = conn.query(`SELECT menuCategoryName FROM item_menuCategory_data WHERE menuCategoryName = '${data.menuCategoryName}'`, function (err, row) {
                                if (err) {
                                    conn.rollback(() => {
                                        conn.release();
                                        console.error("An error occurred in SQL Queery", err);
                                        return res.status(500).send('Database Error');
                                    })
                                } else if (row && row.length) {
                                    conn.rollback(() => {
                                        conn.release();
                                        return res.status(400).send('Category is Already In Use');
                                    })
                                } else {
                                    const sql_querry_addCategory = `INSERT INTO item_menuCategory_data (menuCategoryId, menuCategoryName)  
                                                                    VALUES ('${menuCategoryId}','${data.menuCategoryName}')`;
                                    conn.query(sql_querry_addCategory, (err, data) => {
                                        if (err) {
                                            conn.rollback(() => {
                                                conn.release();
                                                console.error("An error occurred in SQL Queery", err);
                                                return res.status(500).send('Database Error');
                                            })
                                        }
                                        else {
                                            const sql_querry_addMenuItem = `INSERT INTO item_unitWisePrice_data(
                                                                                                    menuCategoryId,
                                                                                                    itemId,
                                                                                                    unit,
                                                                                                    price,
                                                                                                    status
                                                                                                )
                                                                SELECT '${menuCategoryId}', itemId, unit, price, status FROM item_unitWisePrice_data 
                                                                WHERE menuCategoryId = '${baseMenuId}'`;
                                            conn.query(sql_querry_addMenuItem, (err, data) => {
                                                if (err) {
                                                    conn.rollback(() => {
                                                        conn.release();
                                                        console.error("An error occurred in SQL Queery", err);
                                                        return res.status(500).send('Database Error');
                                                    })
                                                } else {
                                                    conn.commit((err) => {
                                                        if (err) {
                                                            conn.rollback(() => {
                                                                conn.release();
                                                                return res.status(500).send('Error In Commit');
                                                            })
                                                        } else {
                                                            conn.release();
                                                            return res.status(200).send("Category Added Successfully");
                                                        }
                                                    })
                                                }
                                            })
                                        }
                                    })
                                }
                            })
                        }
                    } else {
                        conn.rollback(() => {
                            conn.release();
                            return res.status(400).send('You are Not Authorised');
                        })
                    }
                } else {
                    conn.rollback(() => {
                        conn.release();
                        return res.status(404).send('Please Login First...!');
                    })
                }
            })
        } catch (error) {
            conn.rollback(() => {
                conn.release();
                console.error('An error occurred', error);
                res.status(500).send('Internal Server Error');
            })
        }
    })
}

// Remove Manufacture Product Category API

const removeMenuCategory = async (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const rights = decoded.id.rights;
            if (rights == 1) {
                const menuCategoryId = req.query.menuCategoryId.trim();
                req.query.menuCategoryId = pool.query(`SELECT menuCategoryId FROM item_menuCategory_data WHERE menuCategoryId = '${menuCategoryId}'`, (err, row) => {
                    if (err) {
                        console.error("An error occurred in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    } else if (baseMenuId == menuCategoryId) {
                        return res.status(400).send('You Can Not Delete Base Menu');
                    } else if (row && row.length) {
                        const sql_querry_removedetails = `DELETE FROM item_menuCategory_data WHERE menuCategoryId = '${menuCategoryId}'`;
                        pool.query(sql_querry_removedetails, (err, data) => {
                            if (err) {
                                console.error("An error occurred in SQL Queery", err);
                                return res.status(500).send('Database Error');
                            }
                            return res.status(200).send("Category Deleted Successfully");
                        })
                    } else {
                        return res.send('CategoryId Not Found');
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

// Update Manufacture Product Category API

const updateMenuCategory = async (req, res) => {
    try {
        const data = {
            menuCategoryId: req.body.menuCategoryId.trim(),
            menuCategoryName: req.body.menuCategoryName.trim()
        }
        if (!data.menuCategoryName) {
            return res.status(400).send("Please Add Category");
        } else if (baseMenuId == data.menuCategoryId) {
            return res.status(400).send('You Can Not Edit Basic Menu');
        } else {
            const sql_querry_updatedetails = `UPDATE item_menuCategory_data SET menuCategoryName = '${data.menuCategoryName}'
                                              WHERE menuCategoryId = '${data.menuCategoryId}'`;
            pool.query(sql_querry_updatedetails, (err, data) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                }
                return res.status(200).send("Category Updated Successfully");
            })
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Copy Price By Menu Category

const copyPriceAndStatusByMenuId = (req, res) => {
    try {
        const sourceId = req.query.sourceId ? req.query.sourceId : null;
        const targetId = req.query.targetId ? req.query.targetId : null;
        const itemSubCategory = req.query.itemSubCategory ? req.query.itemSubCategory : null;
        if (targetId == baseMenuId) {
            return res.status(400).send('You Can Not Target Base Menu');
        } else if (sourceId && targetId) {
            if (itemSubCategory) {
                sql_query_copyData = `UPDATE item_unitWisePrice_data AS target
                                      JOIN item_unitWisePrice_data AS source ON target.itemId = source.itemId
                                      AND target.menuCategoryId = '${targetId}' 
                                      AND target.unit = source.unit
                                      AND source.menuCategoryId = '${sourceId}'
                                      SET target.price = source.price,
                                      target.status = source.status
                                      WHERE target.itemId IN (
                                          SELECT itemId 
                                          FROM item_menuList_data 
                                          WHERE itemSubCategory = '${itemSubCategory}'
                                      )`;
            } else {
                sql_query_copyData = `UPDATE item_unitWisePrice_data AS target
                                      JOIN item_unitWisePrice_data AS source ON target.itemId = source.itemId
                                      AND target.menuCategoryId = '${targetId}' 
                                      AND target.unit = source.unit
                                      AND source.menuCategoryId = '${sourceId}'
                                      SET target.price = source.price,
                                      target.status = source.status`;
            }
            pool.query(sql_query_copyData, (err, copy) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                }
                return res.status(200).send(`Menu Updated Successfully Like ${sourceId}`);
            })
        } else {
            return res.status(404).send('Target Or Source Not Found');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

module.exports = {
    getMenuCategory,
    addMenuCategory,
    removeMenuCategory,
    updateMenuCategory,
    copyPriceAndStatusByMenuId
}