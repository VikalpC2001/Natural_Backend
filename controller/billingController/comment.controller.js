const pool = require('../../database');
const jwt = require("jsonwebtoken");
const pool2 = require('../../databasePool');

// Get Comment List

const getComment = async (req, res) => {
    try {
        var sql_queries_getCategoryTable = `SELECT
                                                bcd.commentId,
                                                bcd.comment
                                            FROM
                                                billing_comment_data AS bcd
                                            ORDER BY bcd.comment`;

        pool.query(sql_queries_getCategoryTable, (err, rows, fields) => {
            if (err) {
                console.error("An error occurred in SQL Queery", err);
                return res.status(500).send('Database Error');;
            } else {
                const data = rows.map((e) => {
                    return e.comment
                })
                return res.status(200).send(data);
            }
        });
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).json('Internal Server Error');
    }
}

// Add Comment API

const addComment = async (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const rights = decoded.id.rights;
            if (rights == 1) {
                const uid1 = new Date();
                const commentId = String("Comment_" + uid1.getTime());

                const data = {
                    comment: req.body.comment.trim(),
                }
                if (!data.comment) {
                    return res.status(400).send("Please Add Comment");
                } else {
                    pool.query(`SELECT comment FROM billing_comment_data WHERE comment = '${data.comment}'`, function (err, row) {
                        if (err) {
                            console.error("An error occurred in SQL Queery", err);
                            return res.status(500).send('Database Error');
                        } else if (row && row.length) {
                            return res.status(400).send('Comment is Already In Use');
                        } else {
                            const sql_querry_addCategory = `INSERT INTO billing_comment_data (commentId, comment)  
                                                            VALUES ('${commentId}','${data.comment}')`;
                            pool.query(sql_querry_addCategory, (err, data) => {
                                if (err) {
                                    console.error("An error occurred in SQL Queery", err);
                                    return res.status(500).send('Database Error');
                                }
                                return res.status(200).send("Comment Added Successfully");
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

// Remove Comment API

const removeComment = async (req, res) => {
    try {
        let token;
        token = req.headers ? req.headers.authorization.split(" ")[1] : null;
        if (token) {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const rights = decoded.id.rights;
            if (rights == 1) {
                const commentId = req.query.commentId.trim();
                req.query.commentId = pool.query(`SELECT commentId FROM billing_comment_data WHERE commentId = '${commentId}'`, (err, row) => {
                    if (err) {
                        console.error("An error occurred in SQL Queery", err);
                        return res.status(500).send('Database Error');
                    }
                    if (row && row.length) {
                        const sql_querry_removedetails = `DELETE FROM billing_comment_data WHERE commentId = '${commentId}'`;
                        pool.query(sql_querry_removedetails, (err, data) => {
                            if (err) {
                                console.error("An error occurred in SQL Queery", err);
                                return res.status(500).send('Database Error');
                            }
                            return res.status(200).send("Comment Deleted Successfully");
                        })
                    } else {
                        return res.send('CommentId Not Found');
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

// Update Comment API

const updateComment = async (req, res) => {
    try {
        const data = {
            commentId: req.body.commentId.trim(),
            comment: req.body.comment.trim()
        }
        if (!data.comment) {
            return res.status(400).send("Please Add Comment");
        } else {
            pool.query(`SELECT comment FROM billing_comment_data WHERE comment = '${data.comment}' AND commentId != '${data.commentId}'`, function (err, row) {
                if (err) {
                    console.error("An error occurred in SQL Queery", err);
                    return res.status(500).send('Database Error');
                } else if (row && row.length) {
                    return res.status(400).send('Comment is Already In Use');
                } else {
                    const sql_querry_updatedetails = `UPDATE billing_comment_data SET comment = '${data.comment}'
                                                  WHERE commentId = '${data.commentId}'`;
                    pool.query(sql_querry_updatedetails, (err, data) => {
                        if (err) {
                            console.error("An error occurred in SQL Queery", err);
                            return res.status(500).send('Database Error');
                        }
                        return res.status(200).send("Comment Updated Successfully");
                    })
                }
            })
        }
    } catch (error) {
        console.error('An error occurred', error);
        res.status(500).send('Internal Server Error');
    }
}

module.exports = {
    getComment,
    addComment,
    removeComment,
    updateComment
}