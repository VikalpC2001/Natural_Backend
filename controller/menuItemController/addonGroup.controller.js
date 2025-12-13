const { query } = require('express');
const pool = require('../../database');
const jwt = require("jsonwebtoken");

// Get addons List

const getAddOnsGroupList = async (req, res) => {
    try {
        const page = req.query.page;
        const numPerPage = req.query.numPerPage;
        const skip = (page - 1) * numPerPage;
        const limit = skip + ',' + numPerPage;
        sql_querry_getCountDetails = `SELECT count(*) as numRows FROM item_addonsGroup_data`;
        pool.query(sql_querry_getCountDetails, (err, rows, fields) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');
            } else {
                const numRows = rows[0].numRows;
                const numPages = Math.ceil(numRows / numPerPage);
                const sql_query_getDetails = `SELECT groupId, groupName, groupGujaratiName FROM item_addonsGroup_data LIMIT ${limit}`;
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
                            return res.status(200).send({ rows, numRows });
                        }
                    }
                });
            }
        })
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Add addons Group

const addAddOnsGroupData = async (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const rights = decoded.id.rights;
            if (rights == 1) {
                const uid1 = new Date();
                const groupId = String("Group_" + uid1.getTime());

                const data = {
                    groupName: req.body.groupName ? req.body.groupName.trim() : null,
                    groupGujaratiName: req.body.groupName ? req.body.groupGujaratiName.trim() : null,
                }
                if (!data.groupName) {
                    return res.status(400).send("Please Add Group");
                } else {
                    pool.query(`SELECT groupName FROM item_addonsGroup_data WHERE groupName = '${data.groupName}'`, function (err, row) {
                        if (err) {
                            console.error("An error occurred in SQL Queery", err);
                            return res.status(500).send('Database Error');
                        }
                        if (row && row.length) {
                            return res.status(400).send('Group is Already In Use');
                        } else {
                            const sql_querry_addCategory = `INSERT INTO item_addonsGroup_data (groupId, groupName, groupGujaratiName)  
                                                            VALUES ('${groupId}','${data.groupName}','${data.groupGujaratiName}')`;
                            pool.query(sql_querry_addCategory, (err, data) => {
                                if (err) {
                                    console.error("An error occurred in SQL Queery", err);
                                    return res.status(500).send('Database Error');
                                }
                                return res.status(200).send("Group Added Successfully");
                            })
                        }
                    })
                }
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

// Remove addons Group

const removeAddOnsGgroupData = async (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const rights = decoded.id.rights;
            if (rights == 1) {
                const groupId = req.query.groupId;
                req.query.groupId = pool.query(`SELECT groupId FROM item_addonsGroup_data WHERE groupId = '${groupId}'`, (err, row) => {
                    if (err) {
                        console.error("An error occurred in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    }
                    if (row && row.length) {
                        const sql_querry_removedetails = `DELETE FROM item_addonsGroup_data WHERE groupId = '${groupId}'`;
                        pool.query(sql_querry_removedetails, (err, data) => {
                            if (err) {
                                console.error("An error occurred in SQL Queery", err);
                                return res.status(500).send('Database Error');
                            }
                            return res.status(200).send("Group Deleted Successfully");
                        })
                    } else {
                        return res.send('GroupId Not Found');
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

// Update addons Group

const updateAddOnsGroupData = async (req, res) => {
    try {
        const data = {
            groupId: req.body.groupId,
            groupName: req.body.groupName ? req.body.groupName.trim() : null,
            groupGujaratiName: req.body.groupName ? req.body.groupGujaratiName.trim() : null
        }
        if (!data.groupName) {
            return res.status(400).send("Please Add Group");
        }
        const sql_querry_updatedetails = `UPDATE 
                                            item_addonsGroup_data 
                                          SET 
                                            groupName = '${data.groupName}',
                                            groupGujaratiName = '${data.groupGujaratiName}'
                                          WHERE groupId = '${data.groupId}'`;
        pool.query(sql_querry_updatedetails, (err, data) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');
            }
            return res.status(200).send("Group Updated Successfully");
        })
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Assign Addon Group To Item

const assignAddonGroup = async (req, res) => {
    try {
        const data = req.body;

        if (!data.groupId || !data.addonItemsArray.length) {
            return res.status(404).send('Please Fill all the Fields....!')
        } else {
            const filteredTrueData = data.addonItemsArray.filter(item => item.status === 1);

            let addAddonWiseItem = filteredTrueData.map((item, index) => {
                let uniqueId = `uwag_${Date.now() + index + '_' + index}`; // Generating a unique ID using current timestamp
                return `('${uniqueId}', ${item.uwpId}, '${data.groupId}')`;
            }).join(', ');

            let uncheckedId = data.addonItemsArray.map((e) => e.uwpId).join(',');

            let sql_query_removeOld = `DELETE FROM item_unitWiseAddOnsGroup_data
                                       WHERE uwpId IN (${uncheckedId ? uncheckedId : null}) AND groupId = '${data.groupId}'`;

            let sql_query_addItems = `INSERT INTO item_unitWiseAddOnsGroup_data(uwagId, uwpId, groupId)
                                      VALUES ${addAddonWiseItem}`

            pool.query(sql_query_removeOld, (err, raw) => {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else {
                    if (filteredTrueData.length) {
                        pool.query(sql_query_addItems, (err, chk) => {
                            if (err) {
                                console.error("An error occurred in SQL Queery", err);
                                return res.status(500).send('Database Error');
                            } else {
                                return res.status(200).send('Success');
                            }
                        })
                    } else {
                        return res.status(200).send('Success');
                    }
                }
            })
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

// Get Item with Addons Group

const getItemListByAddon = (req, res) => {
    try {
        var date = new Date(), y = date.getFullYear(), m = (date.getMonth());
        var firstDay = new Date(y, m, 1).toString().slice(4, 15);
        var lastDay = new Date(y, m + 1, 0).toString().slice(4, 15);

        const data = {
            subCategoryId: req.query.subCategoryId ? req.query.subCategoryId : null,
            menuCategoryId: req.query.menuCategoryId ? req.query.menuCategoryId : null,
            groupId: req.query.groupId ? req.query.groupId : null
        }
        if (!data.groupId || !data.menuCategoryId) {
            return res.status(404).send('Group Id Or MenuId Not Found')
        }
        let sql_querry_getDetails = `SELECT
                                         uwi.itemId,
                                         uwi.uwpId,
                                         CONCAT(item.itemName,' (',uwi.unit,')') AS itemName,
                                         item.itemSubCategory,
                                         iscd.subCategoryName,
                                         CASE
                                            WHEN uwag.groupId IS NOT NULL THEN true
                                            ELSE false
	                                     END AS status
                                     FROM
                                         item_unitWisePrice_data AS uwi
                                     INNER JOIN item_menuList_data AS item ON item.itemId = uwi.itemId
                                     INNER JOIN item_subCategory_data AS iscd ON iscd.subCategoryId = item.itemSubCategory
                                     LEFT JOIN item_unitWiseAddOnsGroup_data AS uwag ON uwag.uwpId = uwi.uwpId AND uwag.groupId = '${data.groupId}'
                                     WHERE uwi.menuCategoryId = '${data.menuCategoryId}' ${data.subCategoryId ? `AND iscd.subCategoryId = '${data.subCategoryId}'` : ''}
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
                        acc[key] = [];
                    }
                    acc[key].push({
                        uwpId: item.uwpId,
                        itemName: item.itemName,
                        status: item.status
                    });
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

module.exports = {
    getAddOnsGroupList,
    addAddOnsGroupData,
    removeAddOnsGgroupData,
    updateAddOnsGroupData,
    getItemListByAddon,
    assignAddonGroup
}