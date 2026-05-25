const express = require('express');
const path = require('path');
const mysql = require('mysql2/promise');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;
const ATTENDANCE_THRESHOLD = Number(process.env.ATTENDANCE_THRESHOLD) || 75;

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
  region: "us-east-1",
  // Hum explicitly koi credentials pass nahi kar rahe 
  // taake SDK automatically Instance Profile/IAM Role use kare.
});



async function getStudentAttendancePercentage(studentId) {
  const [rows] = await pool.query(
    `SELECT
       COUNT(*) AS total_days,
       SUM(CASE WHEN status = 'Present' THEN 1 ELSE 0 END) AS present_days
     FROM Attendance
     WHERE student_id = ?`,
    [studentId]
  );

  const total = Number(rows[0].total_days);
  const present = Number(rows[0].present_days);

  if (total === 0) return { total: 0, present: 0, percentage: 100 };

  const percentage = Math.round((present / total) * 100);
  return { total, present, percentage };
}

async function publishLowAttendanceAlert({ studentId, studentName, percentage, present, total }) {
  const topicArn = process.env.LOW_ATTENDANCE_SNS_TOPIC_ARN;
  if (!topicArn) {
    console.warn('LOW_ATTENDANCE_SNS_TOPIC_ARN is not set — skipping SNS alert');
    return;
  }

  const message =
    `Low Attendance Alert\n\n` +
    `Student: ${studentName} (ID: ${studentId})\n` +
    `Attendance: ${percentage}% (${present} present out of ${total} days)\n` +
    `Threshold: below ${ATTENDANCE_THRESHOLD}%\n` +
    `Action required: Please review this student's attendance.`;

  const result = await snsClient.send(
    new PublishCommand({
      TopicArn: topicArn,
      Subject: `Low Attendance Alert — ${studentName}`,
      Message: message,
    })
  );

  console.log('Low attendance SNS sent:', result.MessageId);
  return result;
}

async function checkAndNotifyLowAttendance(studentId) {
  try {
    const [students] = await pool.query(
      'SELECT name FROM Students WHERE student_id = ?',
      [studentId]
    );
    const studentName = students[0]?.name || `Student ${studentId}`;

    const { total, present, percentage } = await getStudentAttendancePercentage(studentId);

    if (total > 0 && percentage < ATTENDANCE_THRESHOLD) {
      await publishLowAttendanceAlert({
        studentId,
        studentName,
        percentage,
        present,
        total,
      });
    }
  } catch (snsErr) {
    console.error('SNS low-attendance alert failed:', snsErr.message);
  }
}

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

    // After saving attendance, check if % dropped below threshold and email via SNS
    await checkAndNotifyLowAttendance(studentId);

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
