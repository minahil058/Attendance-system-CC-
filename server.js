const express = require('express');
const path = require('path');
const mysql = require('mysql2/promise');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

const snsClient = new SNSClient({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

app.get('/api/students', async (req, res) => {
  try {
    const [rows] = await pool.query(
      'SELECT student_id, name, department FROM Students ORDER BY student_id'
    );
    res.json(rows);
  } catch (err) {
    console.error('Failed to load students:', err.sqlMessage || err.message);
    res.status(500).json({ error: 'Failed to load students' });
  }
});

app.post('/mark-attendance', async (req, res) => {
  const { student_id, date, status } = req.body;

  if (student_id == null || !date || !status) {
    return res.status(400).json({ error: 'Please provide student_id, date, and status' });
  }

  const studentId = Number(student_id);
  if (!Number.isInteger(studentId) || studentId < 1) {
    return res.status(400).json({ error: 'student_id must be a positive integer' });
  }

  try {
    const [result] = await pool.execute(
      'INSERT INTO Attendance (student_id, date, status) VALUES (?, ?, ?)',
      [studentId, date, status]
    );

    try {
      const snsData = await snsClient.send(
        new PublishCommand({
          Message: `Attendance marked successfully for Student ID: ${student_id} on ${date}`,
          TopicArn: process.env.SNS_TOPIC_ARN,
        })
      );
      console.log('Notification sent successfully:', snsData.MessageId);
    } catch (snsErr) {
      console.error('SNS Error:', snsErr);
    }

    res.status(201).json({
      message: 'Attendance marked successfully',
      insertId: result.insertId,
    });
  } catch (err) {
    console.error('--- DATABASE ERROR ---');
    console.error('SQL Message:', err.sqlMessage);
    console.error('Code:', err.code);

    if (err.code === 'ER_NO_REFERENCED_ROW_2' || err.errno === 1452) {
      return res.status(400).json({
        error: 'Invalid Student ID',
        detail: 'This student does not exist in the database.',
      });
    }

    if (err.code === 'ER_DUP_ENTRY' || err.errno === 1062) {
      return res.status(409).json({
        error: 'Duplicate Entry',
        detail: 'Attendance for this student already marked for today.',
      });
    }

    res.status(500).json({ error: 'Error saving attendance', detail: err.sqlMessage });
  }
});

app.use(express.static(path.join(__dirname, 'public')));

app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});
