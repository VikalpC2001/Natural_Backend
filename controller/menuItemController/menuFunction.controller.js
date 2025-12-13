const pool = require('../../database');

function generateUpdateQuery(data) {
    let query = 'UPDATE item_unitWisePrice_data\nSET price = CASE\n';

    data.forEach((item) => {
        const { uwpId, price } = item;
        query += `    WHEN uwpId = '${uwpId}' THEN ROUND(${price})\n`;
    });

    query += '    ELSE price\nEND\n';

    const uwpIds = data.map((item) => `'${item.uwpId}'`).join(', ');
    query += `WHERE uwpId IN (${uwpIds});`;

    return query;
}

// Function For Variants and Periods

function getItemVariants(itemId, subCategoryId, menuId, callback) {
    let sql_query_getData = `-- GET ACTIVE VARIANTS
                                SELECT 
                                    uwip.uwpId AS uwpId,
                                    uwip.unit AS unit,
                                    uwip.preferredName AS preferredName,
                                    uwip.price AS price,
                                    uwip.status AS status,
                                    uwaog.groupId AS groupId,
                                    uwg.groupName AS groupName,
                                    iad.addonsId AS addonsId,
                                    iad.addonsName AS addonsName,
                                    iad.addonsGujaratiName AS addonsGujaratiName,
                                    iad.price AS addonPrice,
                                    iad.isActive AS isActive
                                FROM 
                                    item_unitWisePrice_data AS uwip
                                LEFT JOIN item_unitWiseAddOnsGroup_data AS uwaog ON uwaog.uwpId = uwip.uwpId
                                LEFT JOIN item_addonsGroup_data AS uwg ON uwg.groupId = uwaog.groupId
                                LEFT JOIN item_addons_data AS iad ON iad.groupId = uwaog.groupId
                                WHERE uwip.itemId = '${itemId}' AND uwip.menuCategoryId = '${menuId}' AND status = 1
                                ORDER BY FIELD(unit, 'No', '100 Gm', '250 Gm', '500 Gm', '750 Gm','1 Kg');
                            -- GET PERIOD
                                SELECT 
                                startTime, 
                                endTime,
                                TIME_FORMAT(startTime,"%h:%i %p") AS displayStartTime,
                                TIME_FORMAT(endTime,"%h:%i %p") AS displayEndTime
                                FROM item_subCategoryPeriod_data 
                                WHERE subCategoryId = '${subCategoryId}';
                            -- GET ALL VARIANTS
                                SELECT uwpId, unit, preferredName, price, status
                                FROM item_unitWisePrice_data
                                WHERE itemId = '${itemId}' AND menuCategoryId = '${menuId}'`;

    pool.query(sql_query_getData, (err, data) => {
        if (err) {
            console.error("An error occurred in SQL Query", err);
            return callback('Database Error', null);
        }
        // Assuming data is an array of results
        let variantAndPeriod = Object.values(JSON.parse(JSON.stringify(data)));
        if (variantAndPeriod.length != 0) {
            let variantsList = variantAndPeriod[0].length ? variantAndPeriod[0] : [];

            // Grouping by unit and then by groupId
            const groupedData = variantsList.reduce((acc, item) => {
                const unitKey = item.unit; // Group by unit
                if (!acc[unitKey]) {
                    acc[unitKey] = {
                        uwpId: item.uwpId,
                        unit: item.unit,
                        preferredName: item.preferredName,
                        price: item.price,
                        status: item.status,
                        addOnsList: {}
                    };
                }

                // Ensure valid group and addon information before processing
                if (item.groupId && item.addonsId) {
                    const groupKey = item.groupId;
                    if (!acc[unitKey].addOnsList[groupKey]) {
                        acc[unitKey].addOnsList[groupKey] = {
                            groupId: item.groupId,
                            groupName: item.groupName,
                            addOnArray: []
                        };
                    }
                    acc[unitKey].addOnsList[groupKey].addOnArray.push({
                        addonsId: item.addonsId,
                        addonsName: item.addonsName,
                        addonsGujaratiName: item.addonsGujaratiName,
                        addonPrice: item.addonPrice,
                        isActive: item.isActive
                    });
                }

                return acc;
            }, {});

            // Transform to the desired structure
            const result = Object.values(groupedData).map(unit => ({
                uwpId: unit.uwpId,
                unit: unit.unit,
                preferredName: unit.preferredName,
                price: unit.price,
                status: unit.status,
                addOnsList: Object.keys(unit.addOnsList).length > 0 ? Object.values(unit.addOnsList) : [] // Check for empty addOnsList
            }));

            let variantsAllList = variantAndPeriod[2].length ? variantAndPeriod[2] : [];
            let jsonData = {
                varients: result.sort((a, b) => (a.unit == "NO") ? -1 : (b.unit == "NO") ? 1 : 0),
                allVariantsList: variantsAllList.length ? variantsAllList.sort((a, b) => (a.unit == "NO") ? -1 : (b.unit == "NO") ? 1 : 0) : [],
                periods: variantAndPeriod[1].length ? variantAndPeriod[1] : [],
                status: variantsList.length ? true : false
            }
            callback(null, jsonData);
        } else {
            let jsonData = {
                varients: [],
                periods: [],
                status: false
            }
            callback(null, jsonData);
        }
    });
}

function varientsAsync(itemId, subCategoryId, menuId) {
    return new Promise((resolve, reject) => {
        getItemVariants(itemId, subCategoryId, menuId, (err, newJson) => {
            if (err) {
                console.error(err);
                reject(err);
            } else {
                resolve(newJson);
            }
        });
    });
}

async function varientDatas(datas, menuId) {
    const updatedDatas = [];
    for (const e of datas) {
        try {
            const newJson = await varientsAsync(e.itemId, e.itemSubCategory, menuId);
            updatedDatas.push(newJson);
        } catch (error) {
            // Handle errors here
            console.error(error);
        }
    }
    return updatedDatas;
}

// Function For Periods

function getPeriods(subCategoryId, callback) {
    let sql_query_getData = `-- GET PERIOD
                                SELECT startTime, endTime 
                                FROM item_subCategoryPeriod_data 
                                WHERE subCategoryId = '${subCategoryId}'`;

    pool.query(sql_query_getData, (err, data) => {
        if (err) {
            console.error("An error occurred in SQL Query", err);
            return callback('Database Error', null);
        }
        // Assuming data is an array of results
        let period = Object.values(JSON.parse(JSON.stringify(data)));
        if (period.length != 0) {
            let jsonData = {
                periods: period.length ? period : []
            }
            callback(null, jsonData);
        } else {
            let jsonData = {
                periods: []
            }
            callback(null, jsonData);
        }
    });
}

function periodAsync(subCategoryId) {
    return new Promise((resolve, reject) => {
        getPeriods(subCategoryId, (err, newJson) => {
            if (err) {
                console.error(err);
                reject(err);
            } else {
                resolve(newJson);
            }
        });
    });
}

async function periodDatas(datas) {
    const updatedDatas = [];
    for (const e of datas) {
        try {
            const newJson = await periodAsync(e.subCategoryId);
            updatedDatas.push(newJson);
        } catch (error) {
            // Handle errors here
            console.error(error);
        }
    }
    return updatedDatas;
}

module.exports = {
    generateUpdateQuery,
    varientDatas,
    periodDatas
}