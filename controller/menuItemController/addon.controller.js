const pool = require('../../database');
const pool2 = require('../../databasePool');
const jwt = require("jsonwebtoken");

// Get addons List

const getAddOnsGroupList = async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const numPerPage = parseInt(req.query.numPerPage) || 10;
        const skip = (page - 1) * numPerPage;

        // Get total groups count
        const countQuery = `SELECT COUNT(*) as numRows FROM item_addonsGroup_data`;
        pool.query(countQuery, (err, countRows) => {
            if (err) {
                console.error("Error in SQL Count Query", err);
                return res.status(500).send("Database Error");
            }

            const numRows = countRows[0].numRows;
            const numPages = Math.ceil(numRows / numPerPage);

            if (numRows === 0) {
                return res.status(200).send({ rows: [], numRows, numPages });
            }

            // Single JOIN query to get groups + addons in one go
            const sql_query_getData = `SELECT 
                                            g.groupId AS groupId,
                                            g.groupName AS groupName, 
                                            g.groupGujaratiName AS groupGujaratiName,
                                            a.addonsId AS addonId, 
                                            a.addonsName AS addonName, 
                                            a.addonsGujaratiName AS addonsGujaratiName, 
                                            a.price AS price, 
                                            a.isActive AS isActive
                                       FROM item_addonsGroup_data g
                                       LEFT JOIN item_addons_data a ON g.groupId = a.groupId
                                       LIMIT ?, ?`;

            pool.query(sql_query_getData, [skip, numPerPage], (err, rows) => {
                if (err) {
                    console.error("Error in SQL Query", err);
                    return res.status(500).send("Database Error");
                }

                // Reshape into required format
                const groupMap = {};
                rows.forEach(row => {
                    if (!groupMap[row.groupId]) {
                        groupMap[row.groupId] = {
                            groupId: row.groupId,
                            groupName: row.groupName,
                            groupGujaratiName: row.groupGujaratiName,
                            addonList: []
                        };
                    }
                    if (row.addonId) {
                        groupMap[row.groupId].addonList.push({
                            addonId: row.addonId,
                            addonName: row.addonName,
                            addonsGujaratiName: row.addonsGujaratiName,
                            price: row.price,
                            isActive: row.isActive
                        });
                    }
                });

                const result = Object.values(groupMap);
                return res.status(200).send({ rows: result, numRows });
            });
        });
    } catch (error) {
        console.error("An error occurred", error);
        res.status(500).json("Internal Server Error");
    }
};

// Add addons 

const addAddonGroupData = (req, res) => {
    pool2.getConnection((err, connection) => {
        if (err) {
            console.error("Error getting database connection:", err);
            return res.status(500).send("Database Error");
        }

        connection.beginTransaction((err) => {
            if (err) {
                console.error("Error beginning transaction:", err);
                connection.release();
                return res.status(500).send("Database Error");
            }

            let token = req.headers?.authorization?.split(" ")[1] || null;
            if (!token) {
                connection.rollback(() => {
                    connection.release();
                    return res.status(404).send("Please Login First....!");
                });
            } else {
                try {
                    const decoded = jwt.verify(token, process.env.JWT_SECRET);
                    const rights = decoded.id.rights;

                    if (rights != 1) {
                        connection.rollback(() => {
                            connection.release();
                            return res.status(400).send("You are Not Authorised");
                        });
                    } else {
                        const addonData = req.body;
                        if (
                            !addonData.groupName?.trim() ||
                            !Array.isArray(addonData.addonList) ||
                            addonData.addonList.length === 0 ||
                            addonData.addonList.some(a => !a.addonName?.trim() || a.price == null || a.price < 0) ||
                            new Set(addonData.addonList.map(a => a.addonName?.trim())).size !== addonData.addonList.length ||
                            new Set(addonData.addonList.filter(a => a.addonsGujaratiName?.trim()).map(a => a.addonsGujaratiName?.trim())).size !== addonData.addonList.filter(a => a.addonsGujaratiName?.trim()).length
                        ) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(400).send("Validation Error");
                            });
                        } else {
                            const uid1 = new Date();
                            const groupId = "Group_" + uid1.getTime();
                            const sql_query_chkgroupName = `SELECT groupId FROM item_addonsGroup_data WHERE groupName = ? OR groupGujaratiName = ?`;
                            connection.query(sql_query_chkgroupName, [addonData.groupName, addonData.groupGujaratiName?.trim() || null], (err, groupRows) => {
                                if (err) {
                                    console.error("Error checking groupName:", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send("Database Error");
                                    });
                                } else if (groupRows.length > 0) {
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(400).send("Group Name already exists");
                                    });
                                } else {
                                    const addonNames = addonData.addonList.map(a => a.addonName.trim());
                                    const addonGujaratiNames = addonData.addonList
                                        .map(a => a.addonsGujaratiName?.trim())
                                        .filter(Boolean);

                                    const checkQuery = `SELECT addonsId FROM item_addons_data WHERE addonsName IN (?) OR addonsGujaratiName IN (?)`;
                                    connection.query(checkQuery,
                                        [addonNames, addonGujaratiNames.length > 0 ? addonGujaratiNames : [""]],
                                        (err, addonRows) => {
                                            if (err) {
                                                console.error("Error checking addons:", err);
                                                connection.rollback(() => {
                                                    connection.release();
                                                    return res.status(500).send("Database Error");
                                                });
                                            } else if (addonRows.length > 0) {
                                                connection.rollback(() => {
                                                    connection.release();
                                                    return res.status(400).send("Addon Name or Gujarati Name already exists");
                                                });
                                            } else {
                                                const sql_query_addGroupName = `INSERT INTO item_addonsGroup_data (groupId, groupName, groupGujaratiName) VALUES (?, ?, ?)`;
                                                connection.query(sql_query_addGroupName,
                                                    [groupId, addonData.groupName, addonData.groupGujaratiName?.trim() || null],
                                                    (err) => {
                                                        if (err) {
                                                            console.error("Error inserting group:", err);
                                                            connection.rollback(() => {
                                                                connection.release();
                                                                return res.status(500).send("Database Error");
                                                            });
                                                        } else {
                                                            const addonValues = addonData.addonList.map(a => [
                                                                "addOns_" + Date.now() + "_" + Math.floor(Math.random() * 1000),
                                                                groupId,
                                                                a.addonName.trim(),
                                                                a.addonsGujaratiName?.trim() || null,
                                                                a.price,
                                                                a.isActive ? 1 : 0
                                                            ]);

                                                            const sql_query_addAddonsData = `INSERT INTO item_addons_data (addonsId, groupId, addonsName, addonsGujaratiName, price, isActive) VALUES ?`;
                                                            connection.query(sql_query_addAddonsData,
                                                                [addonValues],
                                                                (err) => {
                                                                    if (err) {
                                                                        console.error("Error inserting addons:", err);
                                                                        connection.rollback(() => {
                                                                            connection.release();
                                                                            return res.status(500).send("Database Error");
                                                                        });
                                                                    } else {
                                                                        connection.commit((err) => {
                                                                            if (err) {
                                                                                console.error("Error committing transaction:", err);
                                                                                connection.rollback(() => {
                                                                                    connection.release();
                                                                                    return res.status(500).send("Database Error");
                                                                                });
                                                                            } else {
                                                                                connection.release();
                                                                                return res.status(200).send("Addons Data Inserted Successfully");
                                                                            }
                                                                        });
                                                                    }
                                                                }
                                                            );
                                                        }
                                                    }
                                                );
                                            }
                                        }
                                    );
                                }
                            }
                            );
                        }
                    }
                } catch (error) {
                    console.error("JWT/Other Error:", error);
                    connection.rollback(() => {
                        connection.release();
                        return res.status(401).send("Invalid Token");
                    });
                }
            }
        });
    });
};

// Update Addons

const updateAddonGroupData = (req, res) => {
    pool2.getConnection((err, connection) => {
        if (err) {
            console.error("Error getting database connection:", err);
            return res.status(500).send("Database Error");
        }

        connection.beginTransaction((err) => {
            if (err) {
                console.error("Error beginning transaction:", err);
                connection.release();
                return res.status(500).send("Database Error");
            }

            let token = req.headers?.authorization?.split(" ")[1] || null;
            if (!token) {
                connection.rollback(() => {
                    connection.release();
                    return res.status(404).send("Please Login First....!");
                });
            } else {
                try {
                    const decoded = jwt.verify(token, process.env.JWT_SECRET);
                    const rights = decoded.id.rights;

                    if (rights != 1) {
                        connection.rollback(() => {
                            connection.release();
                            return res.status(400).send("You are Not Authorised");
                        });
                    } else {
                        const addonData = req.body;

                        if (
                            !addonData.groupId?.trim() ||
                            !addonData.groupName?.trim() ||
                            !Array.isArray(addonData.addonList) ||
                            addonData.addonList.length === 0 ||
                            addonData.addonList.some(a => !a.addonName?.trim() || a.price == null || a.price < 0) ||
                            new Set(addonData.addonList.map(a => a.addonName?.trim())).size !== addonData.addonList.length ||
                            new Set(addonData.addonList.filter(a => a.addonsGujaratiName?.trim()).map(a => a.addonsGujaratiName?.trim())).size !== addonData.addonList.filter(a => a.addonsGujaratiName?.trim()).length
                        ) {
                            connection.rollback(() => {
                                connection.release();
                                return res.status(400).send("Validation Error");
                            });
                        } else {
                            // Step 1: Check if groupId exists
                            const sql_query_chkGroupId = `SELECT groupId FROM item_addonsGroup_data WHERE groupId = ?`;
                            connection.query(sql_query_chkGroupId, [addonData.groupId], (err, rows) => {
                                if (err) {
                                    console.error("Error checking groupId:", err);
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(500).send("Database Error");
                                    });
                                } else if (rows.length === 0) {
                                    connection.rollback(() => {
                                        connection.release();
                                        return res.status(404).send("Group not found");
                                    });
                                } else {
                                    // Step 2: Check if groupName conflict
                                    const sql_query_chkGroupName = `SELECT groupId FROM item_addonsGroup_data 
                                                                    WHERE (groupName = ? OR groupGujaratiName = ?) AND groupId != ?`;
                                    connection.query(sql_query_chkGroupName,
                                        [addonData.groupName, addonData.groupGujaratiName?.trim() || null, addonData.groupId],
                                        (err, groupRows) => {
                                            if (err) {
                                                console.error("Error checking groupName:", err);
                                                connection.rollback(() => {
                                                    connection.release();
                                                    return res.status(500).send("Database Error");
                                                });
                                            } else if (groupRows.length > 0) {
                                                connection.rollback(() => {
                                                    connection.release();
                                                    return res.status(400).send("Group Name already exists");
                                                });
                                            } else {
                                                // Step 3: Check addonName conflicts with other groups
                                                const addonNames = addonData.addonList.map(a => a.addonName.trim());
                                                const addonGujaratiNames = addonData.addonList
                                                    .map(a => a.addonsGujaratiName?.trim())
                                                    .filter(Boolean);

                                                const checkQuery = `SELECT addonsId FROM item_addons_data 
                                                                    WHERE (addonsName IN (?) OR addonsGujaratiName IN (?)) AND groupId != ?`;

                                                connection.query(checkQuery,
                                                    [addonNames, addonGujaratiNames.length > 0 ? addonGujaratiNames : [""], addonData.groupId],
                                                    (err, addonRows) => {
                                                        if (err) {
                                                            console.error("Error checking addons:", err);
                                                            connection.rollback(() => {
                                                                connection.release();
                                                                return res.status(500).send("Database Error");
                                                            });
                                                        } else if (addonRows.length > 0) {
                                                            connection.rollback(() => {
                                                                connection.release();
                                                                return res.status(400).send("Addon Name or Gujarati Name already exists in another group");
                                                            });
                                                        } else {
                                                            // Step 4: Update group
                                                            const sql_update_group = `UPDATE item_addonsGroup_data 
                                                                                      SET groupName = ?, groupGujaratiName = ? 
                                                                                      WHERE groupId = ?`;
                                                            connection.query(sql_update_group,
                                                                [addonData.groupName, addonData.groupGujaratiName ? addonData.groupGujaratiName : null, addonData.groupId],
                                                                (err) => {
                                                                    if (err) {
                                                                        console.error("Error updating group:", err);
                                                                        connection.rollback(() => {
                                                                            connection.release();
                                                                            return res.status(500).send("Database Error");
                                                                        });
                                                                    } else {
                                                                        // Step 5: Update/Add/Delete addons
                                                                        const clientAddonIds = addonData.addonList
                                                                            .filter(a => a.addonId)
                                                                            .map(a => a.addonId);

                                                                        // 5a: Delete addons not in new list
                                                                        const sql_delete_unused = `DELETE FROM item_addons_data 
                                                                                                   WHERE groupId = ? AND addonsId NOT IN (?)`;
                                                                        connection.query(sql_delete_unused,
                                                                            [addonData.groupId, clientAddonIds.length > 0 ? clientAddonIds : ["dummy"]],
                                                                            (err) => {
                                                                                if (err) {
                                                                                    console.error("Error deleting old addons:", err);
                                                                                    connection.rollback(() => {
                                                                                        connection.release();
                                                                                        return res.status(500).send("Database Error");
                                                                                    });
                                                                                } else {
                                                                                    // 5b: Update existing addons
                                                                                    const updatePromises = addonData.addonList
                                                                                        .filter(a => a.addonId)
                                                                                        .map(a => new Promise((resolve, reject) => {
                                                                                            const sql_update_addon = `UPDATE item_addons_data 
                                                                                                                      SET addonsName = ?, addonsGujaratiName = ?, price = ?, isActive = ? 
                                                                                                                      WHERE addonsId = ? AND groupId = ?`;
                                                                                            connection.query(sql_update_addon,
                                                                                                [a.addonName.trim(), a.addonsGujaratiName?.trim() || null, a.price, a.isActive ? 1 : 0, a.addonId, addonData.groupId],
                                                                                                (err) => err ? reject(err) : resolve()
                                                                                            );
                                                                                        }));

                                                                                    // 5c: Insert new addons
                                                                                    const newAddons = addonData.addonList.filter(a => !a.addonId);
                                                                                    const insertValues = newAddons.map(a => [
                                                                                        "addOns_" + Date.now() + "_" + Math.floor(Math.random() * 1000),
                                                                                        addonData.groupId,
                                                                                        a.addonName.trim(),
                                                                                        a.addonsGujaratiName?.trim() || null,
                                                                                        a.price,
                                                                                        a.isActive ? 1 : 0
                                                                                    ]);

                                                                                    const insertPromise = insertValues.length > 0
                                                                                        ? new Promise((resolve, reject) => {
                                                                                            const sql_insert_addons = `INSERT INTO item_addons_data (addonsId, groupId, addonsName, addonsGujaratiName, price, isActive) 
                                                                                                                       VALUES ?`;
                                                                                            connection.query(sql_insert_addons, [insertValues], (err) => err ? reject(err) : resolve());
                                                                                        })
                                                                                        : Promise.resolve();

                                                                                    Promise.all([...updatePromises, insertPromise])
                                                                                        .then(() => {
                                                                                            connection.commit((err) => {
                                                                                                if (err) {
                                                                                                    console.error("Error committing transaction:", err);
                                                                                                    connection.rollback(() => {
                                                                                                        connection.release();
                                                                                                        return res.status(500).send("Database Error");
                                                                                                    });
                                                                                                } else {
                                                                                                    connection.release();
                                                                                                    return res.status(200).send("Addons Data Updated Successfully");
                                                                                                }
                                                                                            });
                                                                                        }).catch((err) => {
                                                                                            console.error("Error updating/inserting addons:", err);
                                                                                            connection.rollback(() => {
                                                                                                connection.release();
                                                                                                return res.status(500).send("Database Error");
                                                                                            });
                                                                                        });
                                                                                }
                                                                            });
                                                                    }
                                                                }
                                                            );
                                                        }
                                                    }
                                                );
                                            }
                                        }
                                    );
                                }
                            });
                        }
                    }
                } catch (error) {
                    console.error("JWT/Other Error:", error);
                    connection.rollback(() => {
                        connection.release();
                        return res.status(401).send("Invalid Token");
                    });
                }
            }
        });
    });
};

// Remove Addons Group Data

const removeAddonGroupData = (req, res) => {
    try {
        const groupId = req.query.groupId ? req.query.groupId : null;
        if (!groupId) {
            return res.status(404).send("groupId Not Found...!");
        } else {
            const sql_chk_isExistGroup = `SELECT groupId FROM item_addonsGroup_data WHERE groupId = ?`;
            pool.query(sql_chk_isExistGroup, [groupId], (err, rows) => {
                if (rows.length === 0) {
                    return res.status(404).send("groupId is Not Exist...!");
                } else {
                    const sql_query_removeGroup = `DELETE FROM item_addonsGroup_data WHERE groupId = ?`;
                    pool.query(sql_query_removeGroup, [groupId], (err) => {
                        if (err) {
                            console.error("Error in remove Group:", err);
                            return res.status(500).send("Database Error");
                        } else {
                            return res.status(200).send("Addons Remove Success");
                        }
                    })
                }
            })
        }
    } catch (error) {
        console.error("An error occurred", error);
        res.status(500).json("Internal Server Error");
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
                let uniqueId = `iwb_${Date.now() + index + '_' + index}`;
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
    addAddonGroupData,
    updateAddonGroupData,
    removeAddonGroupData,
    getItemListByAddon,
    assignAddonGroup
}