const pool = require('../../database');
const jwt = require("jsonwebtoken");
const { generateToken } = require('../../utils/genrateToken');

/**
 * Function to modify a decoded JWT token by adding an ID to its payload and re-encode it.
 * @param {object} decodedToken - The decoded JWT token's payload.
 * @param {string} id - The ID to be added to the JWT payload.
 * @param {string} secret - The secret key used for JWT encoding/decoding.
 * @returns {string|null} - Returns the modified JWT token or null if an error occurs.
 */

function modifyDecodedJwt(decodedToken, secret) {
    try {
        // Re-encode the modified payload into a JWT token using the same secret key and expiry time
        const modifiedToken = jwt.sign(decodedToken, secret);

        return modifiedToken;
    } catch (error) {
        console.error('Error modifying JWT:', error.message);
        return null; // Return null if an error occurs
    }
}

// test api
const getNewTokenByBranchId = (req, res) => {
    try {
        let token;
        token = req.headers && req.headers.authorization ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const originalJwtToken = token;
            const idToAdd = req.body.idToAdd;
            const branchName = req.body.branchName;
            const secretKey = process.env.JWT_SECRET;

            if (!originalJwtToken || !idToAdd || !secretKey) {
                return res.status(404).send('Please Fill All The Fields....!')
            }

            // Decode the JWT token to get its payload
            let decodedPayload = jwt.verify(originalJwtToken, secretKey);
            decodedPayload = { ...decodedPayload, id: { ...decodedPayload.id, branchId: idToAdd } }

            // Modify the decoded payload and get the new JWT token
            const modifiedJwtToken = modifyDecodedJwt(decodedPayload, secretKey);

            if (modifiedJwtToken) {
                return res.status(200).send({ ...decodedPayload.id, branchName: branchName, token: modifiedJwtToken });
            } else {
                return res.status(200).send('Failed to modify JWT token.');
            }
        } else {
            res.status(401).send("Please Login Firest.....!");
        }
    } catch (error) {
        console.error('Error decoding JWT:', error.message);
        return res.status(500).send('Error decoding JWT');
    }
}

// Get New Token By Manu Facture Product Category

const getNewTokenByMfProductCategoryId = (req, res) => {
    try {
        let token;
        token = req.headers && req.headers.authorization ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const originalJwtToken = token;
            const categoryId = req.body.categoryId;
            const categoryName = req.body.categoryName;
            const secretKey = process.env.JWT_SECRET;

            if (!originalJwtToken || !categoryId || !secretKey) {
                return res.status(404).send('Please Fill All The Fields....!')
            }

            // Decode the JWT token to get its payload
            let decodedPayload = jwt.verify(originalJwtToken, secretKey);
            decodedPayload = { ...decodedPayload, id: { ...decodedPayload.id, categoryId: categoryId } }

            // Modify the decoded payload and get the new JWT token
            const modifiedJwtToken = modifyDecodedJwt(decodedPayload, secretKey);

            if (modifiedJwtToken) {
                return res.status(200).send({ ...decodedPayload.id, categoryName: categoryName, token: modifiedJwtToken });
            } else {
                return res.status(200).send('Failed to modify JWT token.');
            }
        } else {
            res.status(401).send("Please Login Firest.....!");
        }
    } catch (error) {
        console.error('Error decoding JWT:', error.message);
        return res.status(500).send('Error decoding JWT');
    }
}

// Get User API

const getUserDetails = async (req, res) => {
    try {
        let token;
        token = req.headers.authorization ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const userId = decoded.id.id;
            const branchId = decoded.id.branchId;
            const userRights = decoded.id.rights;
            if (userRights == 1 || userRights == 2) {
                const page = req.query.page;
                const numPerPage = req.query.numPerPage;
                const skip = (page - 1) * numPerPage;
                const limit = skip + ',' + numPerPage;
                if (branchId) {
                    sql_querry_getdetails = `SELECT count(*) as numRows FROM user_details WHERE user_details.userId NOT IN ('${userId}') AND user_details.branchId = '${branchId}'`;
                } else {
                    sql_querry_getdetails = `SELECT count(*) as numRows FROM user_details WHERE user_details.userId NOT IN ('${userId}')`;
                }
                pool.query(sql_querry_getdetails, (err, rows, fields) => {
                    if (err) {
                        console.error("An error occurd in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    } else {
                        const numRows = rows[0].numRows;
                        const numPages = Math.ceil(numRows / numPerPage);
                        if (branchId) {
                            sql_querry_getdetail = `SELECT userId, CONCAT(userFirstName,' ',userLastName) AS userFullName, userGender, userName, password, emailAddress, user_rights.rightsName FROM user_details
                                                     INNER JOIN user_rights ON user_rights.rightsId = user_details.userRights
                                                     WHERE user_details.userId NOT IN ('${userId}') AND user_details.branchId = '${branchId}'
                                                     ORDER BY user_rights.positionNumber LIMIT ${limit}`;
                            console.log('1');
                        } else {
                            sql_querry_getdetail = `SELECT userId, CONCAT(userFirstName,' ',userLastName) AS userFullName, userGender, userName, password, emailAddress, user_rights.rightsName, branch_data.branchName AS branchName FROM user_details
                                                     INNER JOIN user_rights ON user_rights.rightsId = user_details.userRights
                                                     LEFT JOIN branch_data ON branch_data.branchId = user_details.branchId
                                                     WHERE user_details.userId NOT IN ('${userId}')
                                                     ORDER BY user_rights.positionNumber LIMIT ${limit}`;
                            console.log('2');
                        }
                        pool.query(sql_querry_getdetail, (err, rows, fields) => {
                            if (err) {
                                console.error("An error occurd in SQL Queery", err);
                                return res.status(500).send('Database Error');;
                            } else {
                                console.log(rows);
                                console.log(numRows);
                                console.log("Total Page :-", numPages);
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

            } else {
                return res.status(400).send('Unauthorised Person');
            }
        } else {
            res.status(401);
            res.send("Please Login Firest.....!");
        }
    } catch (error) {
        console.error('An error occurd', error);
        res.status(500).json('Internal Server Error');
    }
}

// User Login API

const authUser = async (req, res) => {
    try {
        const user = {
            userName: req.body.userName,
            Password: req.body.Password
        };

        const sql_querry_getUserDetails = `SELECT userId, userRights, userFirstName, userLastName, userName, password, user_details.branchId AS branchId, branch_data.branchName AS branchName, userCreationDate
                                           FROM user_details
                                           LEFT JOIN branch_data ON branch_data.branchId = user_details.branchId
                                           WHERE userName = ?`;

        pool.query(sql_querry_getUserDetails, [user.userName], (err, data) => {
            if (err) {
                console.error("SQL Error:", err);
                res.status(500).send("Database Error");
            } else {
                if (!data || data.length === 0) {
                    res.status(400).send("Invalid Username or Password");
                } else {
                    const userData = data[0];

                    if (userData.password !== user.Password) {
                        res.status(400).send("Invalid Username or Password");
                    } else {
                        const creationDate = new Date(userData.userCreationDate);
                        const currentDate = new Date();
                        const diffInDays = Math.floor((currentDate - creationDate) / (1000 * 60 * 60 * 24));

                        const isAdmin = userData.userRights === 1; // change if your admin flag is different

                        if (diffInDays > 60 && !isAdmin) {
                            res.status(403).send("Trial expired. Please contact support.");
                        } else {
                            const tokenPayload = {
                                id: userData.userId,
                                rights: userData.userRights,
                                userName: userData.userFirstName + " " + userData.userLastName,
                                firstName: userData.userFirstName,
                                // branchId: userData.branchId
                                branchId: "branchId_1704373871685"
                            };

                            res.json({
                                userId: userData.userId,
                                userRights: userData.userRights,
                                userName: userData.userFirstName + " " + userData.userLastName,
                                branchId: userData.branchId,
                                branchName: userData.branchName,
                                token: generateToken(tokenPayload)
                            });

                            console.log("Login successful:", tokenPayload, new Date().toLocaleString());
                        }
                    }
                }
            }
        });
    } catch (error) {
        console.error("Server Error:", error);
        res.status(500).send("Internal Server Error");
    }
};


// Rights DDL API

const ddlRights = (req, res) => {
    try {
        sql_querry_ddlRights = `SELECT rightsId, rightsName FROM user_rights ORDER BY positionNumber`;
        pool.query(sql_querry_ddlRights, (err, data) => {
            if (err) {
                console.error("An error occurd in SQL Queery", err);
                return res.status(500).send('Database Error');
            }
            return res.status(200).json(data);
        })
    } catch (error) {
        console.error('An error occurd', error);
        res.status(500).json('Internal Server Error');
    }
}

// Add User By Owner API

const addUserDetailsByOwner = async (req, res) => {
    try {

        let token;
        token = req.headers.authorization ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const userRights = decoded.id.rights;
            if (userRights == 1) {
                const uid1 = new Date();
                const id = String("user_" + uid1.getTime());
                console.log("...", id);

                const data = {
                    userFirstName: req.body.userFirstName.trim(),
                    userLastName: req.body.userLastName.trim(),
                    userGender: req.body.userGender.trim(),
                    userName: req.body.userName.trim(),
                    password: req.body.password.trim(),
                    emailId: req.body.emailId ? req.body.emailId.trim() : null,
                    userRights: req.body.userRights,
                    branchId: req.body.branchId
                }
                if (!data.userFirstName || !data.userLastName || !data.userGender || !data.userName || !data.password || !data.userRights) {
                    res.status(400);
                    res.send("Please Fill all the feilds")
                } else {
                    req.body.userName = pool.query(`SELECT userName FROM user_details WHERE userName = '${data.userName}'`, function (err, row) {
                        if (err) {
                            console.error("An error occurd in SQL Queery", err);
                            return res.status(500).send('Database Error');
                        }
                        if (row && row.length) {
                            return res.status(400).send('userName is Already In Use');
                        } else {
                            const sql_querry_addUser = `INSERT INTO user_details (userId, userFirstName, userLastName, userGender, userName, password, emailAddress, userRights, branchId)  
                                                        VALUES ('${id}','${data.userFirstName}','${data.userLastName}','${data.userGender}','${data.userName}','${data.password}',NULLIF('${data.emailId}','null'),${data.userRights},${data.branchId ? `'${data.branchId}'` : null})`;
                            pool.query(sql_querry_addUser, (err, data) => {
                                if (err) {
                                    console.error("An error occurd in SQL Queery", err);
                                    return res.status(500).send('Database Error');
                                }
                                return res.status(200).send("User Added Successfully");
                            })
                        }
                    })
                }
            } else {
                return res.status(400).send('Unauthorised Person');
            }
        } else {
            res.status(401);
            res.send("Please Login Firest.....!");
        }
    } catch (error) {
        console.error('An error occurd', error);
        res.status(500).json('Internal Server Error');
    }
}

// Add User By Branch Owner

const addUserDetailsByBranchOwner = async (req, res) => {
    try {

        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const userRights = decoded.id.rights;
            const branchId = decoded.id.branchId;
            if (userRights == 1 || userRights == 2) {
                const uid1 = new Date();
                const id = String("user_" + uid1.getTime());
                console.log("...", id);

                const data = {
                    userFirstName: req.body.userFirstName.trim(),
                    userLastName: req.body.userLastName.trim(),
                    userGender: req.body.userGender.trim(),
                    userName: req.body.userName.trim(),
                    password: req.body.password.trim(),
                    emailId: req.body.emailId ? req.body.emailId.trim() : null,
                    userRights: req.body.userRights
                }
                if (!data.userFirstName || !data.userLastName || !data.userGender || !data.userName || !data.password || !data.userRights) {
                    res.status(400);
                    res.send("Please Fill all the feilds")
                } else {
                    req.body.userName = pool.query(`SELECT userName FROM user_details WHERE userName = '${data.userName}'`, function (err, row) {
                        if (err) {
                            console.error("An error occurd in SQL Queery", err);
                            return res.status(500).send('Database Error');
                        }
                        if (row && row.length) {
                            return res.status(400).send('userName is Already In Use');
                        } else {
                            const sql_querry_addUser = `INSERT INTO user_details (userId, userFirstName, userLastName, userGender, userName, password, emailAddress, userRights, branchId)  
                                                        VALUES ('${id}','${data.userFirstName}','${data.userLastName}','${data.userGender}','${data.userName}','${data.password}',NULLIF('${data.emailId}','null'),${data.userRights},'${branchId}')`;
                            pool.query(sql_querry_addUser, (err, data) => {
                                if (err) {
                                    console.error("An error occurd in SQL Queery", err);
                                    return res.status(500).send('Database Error');
                                }
                                return res.status(200).send("User Added Successfully");
                            })
                        }
                    })
                }
            } else {
                return res.status(400).send('Unauthorised Person');
            }
        } else {
            res.status(401);
            res.send("Please Login Firest.....!");
        }
    } catch (error) {
        console.error('An error occurd', error);
        res.status(500).json('Internal Server Error');
    }
}

// Remove User API

const removeUserDetails = async (req, res) => {

    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const userRights = decoded.id.rights;
            if (userRights == 1) {
                userId = req.query.userId
                req.query.userId = pool.query(`SELECT userId FROM user_details WHERE userId= '${userId}'`, (err, row) => {
                    if (err) {
                        console.error("An error occurd in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    }
                    if (row && row.length) {
                        const sql_querry_removedetails = `DELETE FROM user_details WHERE userId = '${userId}'`;
                        pool.query(sql_querry_removedetails, (err, data) => {
                            if (err) {
                                console.error("An error occurd in SQL Queery", err);
                                return res.status(500).send('Database Error');
                            }
                            return res.status(200).send("User Deleted Successfully");
                        })
                    } else {
                        return res.status(400).send('user is Already Deleted');
                    }
                })
            } else {
                return res.status(400).send('Unauthorised Person');
            }
        } else {
            res.status(401);
            res.send("Please Login Firest.....!");
        }
    } catch (error) {
        console.error('An error occurd', error);
        res.status(500).json('Internal Server Error');
    }
}

// Fill User For Update 

const fillUserDetails = (req, res) => {
    try {
        const userId = req.query.userId
        sql_querry_fillUser = `SELECT userId, userFirstName, userLastName, userGender ,userName, password, emailAddress AS emailId, userRights, branchId FROM user_details WHERE userId = '${userId}'`;
        pool.query(sql_querry_fillUser, (err, data) => {
            if (err) {
                console.error("An error occurd in SQL Queery", err);
                return res.status(500).send('Database Error');
            }
            return res.status(200).send(data[0]);
        })
    } catch (error) {
        console.error('An error occurd', error);
        res.status(500).json('Internal Server Error');
    }
}

// Update User API

const updateUserDetailsByOwner = async (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const userRights = decoded.id.rights;
            if (userRights == 1) {
                const data = {
                    userId: req.body.userId,
                    userFirstName: req.body.userFirstName.trim(),
                    userLastName: req.body.userLastName.trim(),
                    userGender: req.body.userGender.trim(),
                    userName: req.body.userName.trim(),
                    password: req.body.password.trim(),
                    emailId: req.body.emailId ? req.body.emailId.trim() : null,
                    userRights: req.body.userRights,
                    branchId: req.body.branchId
                }
                console.log('joo', data);
                const sql_querry_updatedetails = `UPDATE user_details SET userFirstName = '${data.userFirstName}',
                                                                  userLastName = '${data.userLastName}',
                                                                  userGender = '${data.userGender}',
                                                                  userName = '${data.userName}',
                                                                  password = '${data.password}',
                                                                  emailAddress = NULLIF('${data.emailId}','null'),
                                                                  userRights = ${data.userRights} ,
                                                                  branchId = ${data.branchId ? `'${data.branchId}'` : null}
                                                            WHERE userId = '${data.userId}'`;
                pool.query(sql_querry_updatedetails, (err, data) => {
                    if (err) {
                        console.error("An error occurd in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    }
                    return res.status(200).send("User Updated Successfully");
                })
            } else {
                return res.status(400).send('Unauthorised Person');
            }
        } else {
            res.status(401);
            res.send("Please Login Firest.....!");
        }
    } catch (error) {
        console.error('An error occurd', error);
        res.status(500).json('Internal Server Error');
    }
}

const updateUserDetailsBranchOwner = async (req, res) => {
    try {
        let token;
        token = req.headers.authorization ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const userRights = decoded.id.rights;
            const branchId = decoded.id.branchId;
            if (userRights == 1) {
                const data = {
                    userId: req.body.userId,
                    userFirstName: req.body.userFirstName.trim(),
                    userLastName: req.body.userLastName.trim(),
                    userGender: req.body.userGender.trim(),
                    userName: req.body.userName.trim(),
                    password: req.body.password.trim(),
                    emailId: req.body.emailId ? req.body.emailId.trim() : null,
                    userRights: req.body.userRights,
                    branchId: req.body.branchId
                }
                const sql_querry_updatedetails = `UPDATE user_details SET userFirstName = '${data.userFirstName}',
                                                                          userLastName = '${data.userLastName}',
                                                                          userGender = '${data.userGender}',
                                                                          userName = '${data.userName}',
                                                                          password = '${data.password}',
                                                                          emailAddress = NULLIF('${data.emailId}','null'),
                                                                          userRights = ${data.userRights} ,
                                                                          branchId = '${branchId}'
                                                                    WHERE userId = '${data.userId}'`;
                pool.query(sql_querry_updatedetails, (err, data) => {
                    if (err) {
                        console.error("An error occurd in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    }
                    return res.status(200).send("User Updated Successfully");
                })
            } else {
                return res.status(400).send('Unauthorised Person');
            }
        } else {
            res.status(401);
            res.send("Please Login Firest.....!");
        }
    } catch (error) {
        console.error('An error occurd', error);
        res.status(500).json('Internal Server Error');
    }
}

// Authenticate Password

const chkPassword = (req, res) => {
    try {
        let token;
        token = req.headers.authorization ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const userRights = decoded.id.rights;
            const userId = decoded.id.id;
            if (userRights == 1) {
                const userPassword = req.body.userPassword ? req.body.userPassword : null;
                if (!userPassword) {
                    return res.status(404).send('Password Not Found');
                }
                sql_querry_getUserPassword = `SELECT password AS chkPassword FROM user_details WHERE userId = '${userId}'`;
                pool.query(sql_querry_getUserPassword, (err, pass) => {
                    if (err) {
                        console.error("An error occurd in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    } else {
                        const chkPass = pass && pass[0].chkPassword ? pass[0].chkPassword : null;
                        if (chkPass) {
                            if (chkPass == userPassword) {
                                return res.status(200).send('Password Match Successfully');
                            } else {
                                return res.status(404).send('Wrong Credential');
                            }
                        } else {
                            return res.status(404).send('Password Not Found In Database');
                        }
                    }
                })
            } else {
                return res.status(400).send('Unauthorised Person');
            }
        } else {
            res.status(401);
            res.send("Please Login Firest.....!");
        }
    } catch (error) {
        console.error('An error occurd', error);
        res.status(500).json('Internal Server Error');
    }
}

module.exports = {
    authUser,
    getUserDetails,
    ddlRights,
    chkPassword,
    addUserDetailsByOwner,
    addUserDetailsByBranchOwner,
    removeUserDetails,
    updateUserDetailsByOwner,
    fillUserDetails,
    getNewTokenByBranchId,
    updateUserDetailsBranchOwner,
    getNewTokenByMfProductCategoryId
}