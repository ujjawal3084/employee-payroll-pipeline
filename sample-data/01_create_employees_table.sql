-- Employee/Payroll source table (Amazon RDS PostgreSQL)
-- This is the table DMS will use as the CDC source

CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    department VARCHAR(50),
    designation VARCHAR(50),
    salary NUMERIC(10,2),
    hire_date DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
