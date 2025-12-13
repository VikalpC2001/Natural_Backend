const pool = require('../../database');

const addUnit = (req, res) => {
    try {
        const data = {
            unitName: req.body.unitName.trim()
        }
        if (!data.unitName) {
            return res.status(400).send("Please Add Unit");
        } else {
            req.body.productName = pool.query(`SELECT unit FROM unit_data WHERE unit = '${data.unitName}'`, function (err, row) {
                if (err) {
                    console.error("An error occurd in SQL Queery", err);
                    return res.status(500).send('Database Error');
                }
                if (row && row.length) {
                    return res.status(400).send('Unit is Already In Use');
                } else {
                    const sql_querry_addCategory = `INSERT INTO unit_data (unit)  
                                                    VALUES (LOWER('${data.unitName}'))`;
                    pool.query(sql_querry_addCategory, (err, data) => {
                        if (err) {
                            console.error("An error occurd in SQL Queery", err);
                            return res.status(500).send('Database Error');
                        }
                        return res.status(200).send("Unit Added Successfully");
                    })
                }
            })
        }
    } catch (error) {
        console.error('An error occurd', error);
        res.status(500).json('Internal Server Error');
    }
}

const getUnit = (req, res) => {
    try {
        const sql_query_getUnitData = `SELECT LOWER(unit) AS unitName FROM unit_data ORDER BY unit ASC`;
        pool.query(sql_query_getUnitData, (err, data) => {
            if (err) {
                console.error("An error occurd in SQL Queery", err);
                return res.status(500).send('Database Error');
            }
            let unitArray = data.map((item) => item.unitName);
            return res.status(200).send(unitArray);
        })
    } catch (error) {
        console.error('An error occurd', error);
        res.status(500).json('Internal Server Error');
    }
}

const updateUnit = (req, res) => {
    try {
        const data = {
            preUnitName: req.body.preUnitName.toLowerCase().trim(),
            unitName: req.body.unitName.toLowerCase().trim().trim()
        }
        if (!data.unitName) {
            return res.status(400).send("Please Add Unit");
        } else {
            let sql_query_getDetails = `SELECT unit FROM unit_data WHERE unit != '${data.preUnitName}';
                                        SELECT unit FROM unit_data WHERE unit = '${data.preUnitName}'`;
            req.body.productName = pool.query(sql_query_getDetails, (err, row) => {
                if (err) {
                    console.error("An error occurd in SQL Queery", err);
                    return res.status(500).send('Database Error');
                }
                const preUnit = row && row[1].length ? row[1][0].unit : null;
                const unitArr = Object.values(JSON.parse(JSON.stringify(row[0])));
                const isUnit = unitArr.map((item) => item.unit).includes(`${data.unitName}`);
                console.log(data.unitName);
                if (data.preUnitName == data.unitName) {
                    return res.status(400).send('Unit is Same No Change');
                } else if (preUnit == null) {
                    return res.status(404).send('unitId Not Found');
                } else if (isUnit) {
                    return res.status(400).send('Unit is Already In Use');
                } else {
                    const sql_querry_addCategory = `UPDATE unit_data SET unit = LOWER('${data.unitName}')
                                                    WHERE unit = '${data.preUnitName}'`;
                    pool.query(sql_querry_addCategory, (err, data) => {
                        if (err) {
                            console.error("An error occurd in SQL Queery", err);
                            return res.status(500).send('Database Error');
                        }
                        return res.status(200).send("Unit Updated Successfully");
                    })
                }
            })
        }
    } catch (error) {
        console.error('An error occurd', error);
        res.status(500).json('Internal Server Error');
    }
}

module.exports = {
    addUnit,
    getUnit,
    updateUnit
}