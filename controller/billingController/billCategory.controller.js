const pool = require('../../database');
const jwt = require("jsonwebtoken");

// Get Bill Category Data

const getBillCategory = (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const branchId = decoded.id.branchId;

            let sql_query_getCategory = `SELECT
                                         bwc.bwcId AS bwcId,
                                         bwc.branchId AS branchId,
                                         bwc.categoryId AS categoryId,
                                         bcd.categoryName AS categoryName,
                                         bwc.menuId AS menuId,
                                         imc.menuCategoryName AS menuCategoryName,
                                         bwc.firmId AS firmId,
                                         bfd.firmName AS firmName,
                                         bwc.isOfficial AS isOfficial,
                                         bwc.billFooterNote AS billFooterNote,
                                         bwc.appriciateLine AS appriciateLine,
                                         bwc.categoryStatus AS categoryStatus
                                     FROM
                                         billing_branchWiseCategory_data AS bwc
                                     LEFT JOIN billing_category_data AS bcd ON bcd.categoryId = bwc.categoryId
                                     LEFT JOIN item_menuCategory_data AS imc ON imc.menuCategoryId = bwc.menuId
                                     LEFT JOIN billing_firm_data AS bfd ON bfd.firmId = bwc.firmId
                                     WHERE bwc.branchId = '${branchId}' AND bwc.categoryId IN ('pickUp', 'delivery', 'dineIn')`;
            pool.query(sql_query_getCategory, (err, data) => {
                if (err) {
                    console.error("An error occurred in SQL Query", err);
                    return res.status(500).send('Database Error');
                } else {
                    // Create an object to hold the results
                    const categories = {};

                    // Loop through the results and populate the categories object
                    data.forEach(row => {
                        categories[row.categoryName] = row;
                    });
                    return res.status(200).send(categories);
                }
            });
        } else {
            return res.status(400).send('Please Login First....!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Update Bill Category Data

const updateBillCategoryData = (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const branchId = decoded.id.branchId;

            const data = {
                bwcId: req.body.bwcId ? req.body.bwcId : null,
                categoryId: req.body.categoryId ? req.body.categoryId : null,
                menuId: req.body.menuId ? req.body.menuId : null,
                firmId: req.body.firmId ? req.body.firmId : null,
                isOfficial: req.body.isOfficial ? req.body.isOfficial : false,
                billFooterNote: req.body.billFooterNote ? req.body.billFooterNote : null,
                appriciateLine: req.body.appriciateLine ? req.body.appriciateLine : null,
                categoryStatus: req.body.categoryStatus ? req.body.categoryStatus : 0,
            }
            if (!data.categoryId || !data.menuId || !data.firmId) {
                return res.status(404).send("Pleasr Provide All Fields...!")
            } else {
                let sql_query_updateData = `UPDATE
                                                billing_branchWiseCategory_data
                                            SET
                                                menuId = '${data.menuId}',
                                                firmId = '${data.firmId}',
                                                isOfficial = '${data.isOfficial}',
                                                billFooterNote = '${data.billFooterNote}',
                                                appriciateLine = '${data.appriciateLine}',
                                                categoryStatus = '${data.categoryStatus}'
                                            WHERE bwcId = '${data.bwcId}' AND branchId = '${branchId}'`;
                pool.query(sql_query_updateData, (err, data) => {
                    if (err) {
                        console.error("An error occurred in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    } else {
                        return res.status(200).send('Record Updated Successfully');
                    }
                })
            }
        } else {
            return res.status(400).send('Please Login First....!');
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// DDL Bill Category

const ddlBillCategory = (req, res) => {
    try {
        let sql_query_getCategory = `SELECT
                                         bcd.categoryName AS categoryName
                                     FROM
                                         billing_category_data AS bcd`;
        pool.query(sql_query_getCategory, (err, data) => {
            if (err) {
                console.error("An error occurred in SQL Query", err);
                return res.status(500).send('Database Error');
            } else {
                const categoryArray = data.map((e) => e.categoryName)
                return res.status(200).send(categoryArray);
            }
        });
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

module.exports = {
    getBillCategory,
    updateBillCategoryData,
    ddlBillCategory
}