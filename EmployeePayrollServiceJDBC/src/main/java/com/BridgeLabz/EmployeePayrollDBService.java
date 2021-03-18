package com.BridgeLabz;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EmployeePayrollDBService {

    private int connectionCounter = 0;
    private PreparedStatement employeePayrollDataStatement;
    private static EmployeePayrollDBService employeePayrollDBService;

    private EmployeePayrollDBService() {
    }

    public static EmployeePayrollDBService getInstance() {
        if (employeePayrollDBService == null)
            employeePayrollDBService = new EmployeePayrollDBService();
        return employeePayrollDBService;
    }

    private List<EmployeePayrollData> getEmployeePayrollDataUsingDB(String sql) {
        List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
        try (Connection connection = this.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            employeePayrollList = this.getEmployeePayrollData(resultSet);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return employeePayrollList;
    }

    public List<EmployeePayrollData> readData() {
        String sql = "SELECT * FROM employee_payroll;";
        return this.getEmployeePayrollDataUsingDB(sql);
    }

    private Connection getConnection() {
        connectionCounter++;
        String jdbcURL = "jdbc:mysql://localhost:3306/payroll_service?useSSL=false";
        String userName = "root";
        String password = "password1234";
        Connection connection;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("Driver Loaded.....Connecting to the databasse");
            System.out.println("Processing Thread:" +Thread.currentThread().getName()+
                    "Connecting to databse with id:" +connectionCounter);
            connection = DriverManager.getConnection(jdbcURL, userName, password);
            System.out.println("Processing Thread:" +Thread.currentThread().getName()+
                    "id:" +connectionCounter+ "Connection is sucessful!!!!" +connection);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot Find the driver in the classpath", e);
        }
        return connection;
    }

    public int updateEmployeeData(String name, double salary) {
        return this.updateEmployeeDataUsingStatement(name, salary);
    }

    private int updateEmployeeDataUsingStatement(String name, double salary) {
        String sql = String.format("update employee_payroll set salary = %.2f where name = '%s';", salary, name);
        try (Connection connection = this.getConnection()) {
            Statement statement = connection.createStatement();
            return statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public List<EmployeePayrollData> getEmployeePayrollData(String name) {
        List<EmployeePayrollData> employeePayrollList = null;
        if (this.employeePayrollDataStatement == null)
            this.preparedStatementForEmployeeData();
        try {
            employeePayrollDataStatement.setString(1, name);
            ResultSet resultSet = employeePayrollDataStatement.executeQuery();
            employeePayrollList = this.getEmployeePayrollData(resultSet);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return employeePayrollList;
    }

    private List<EmployeePayrollData> getEmployeePayrollData(ResultSet resultSet) {
        List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
        try {
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                double salary = resultSet.getDouble("salary");
                LocalDate startDate = resultSet.getDate("start").toLocalDate();
                int dept = resultSet.getInt("dept");
                employeePayrollList.add(new EmployeePayrollData(id, dept, name, salary, startDate));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return employeePayrollList;
    }

    private void preparedStatementForEmployeeData() {
        try {
            Connection connection = this.getConnection();
            String sql = "SELECT * FROM employee_payroll Where name = ?";
            employeePayrollDataStatement = connection.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<EmployeePayrollData> getEmployeePayrollForDateRange(LocalDate startDate, LocalDate endDate) {
        String sql = String.format("SELECT * FROM employee_payroll WHERE START BETWEEN '%s' AND '%s';",
                Date.valueOf(startDate), Date.valueOf(endDate));
        return this.getEmployeePayrollDataUsingDB(sql);
    }

    public Map<String, Double> getSalaryByGender(String sql) {
        Map<String, Double> genderToAverageSalaryMap = new HashMap<>();
        try (Connection connection = this.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(sql);
            while (result.next()) {
                String gender = result.getString("gender");
                Double salary = result.getDouble("salary");
                genderToAverageSalaryMap.put(gender, salary);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return genderToAverageSalaryMap;
    }

    public Map<String, Double> getAverageSalaryByGender() {
        String sql = "SELECT gender,AVG(salary) as salary FROM employee_payroll GROUP BY gender;";
        return this.getSalaryByGender(sql);
    }

    public Map<String, Double> getSumOfSalaryByGender() {
        String sql = "SELECT gender,SUM(salary) as salary FROM employee_payroll GROUP BY gender;";
        return this.getSalaryByGender(sql);
    }

    public Map<String, Double> getCountOfSalaryByGender() {
        String sql = "SELECT gender,COUNT(salary) as salary FROM employee_payroll GROUP BY gender;";
        return this.getSalaryByGender(sql);
    }

    public Map<String, Double> getMaxOfSalaryByGender() {
        String sql = "SELECT gender,MAX(salary) as salary FROM employee_payroll GROUP BY gender;";
        return this.getSalaryByGender(sql);
    }

    public Map<String, Double> getMinOfSalaryByGender() {
        String sql = "SELECT gender,Min(salary) as salary FROM employee_payroll GROUP BY gender;";
        return this.getSalaryByGender(sql);
    }

    /*
     * public EmployeePayrollData addEmployeeToPayrollUC7(String name, double
     * salary, LocalDate startDate, String gender) throws SQLException { int
     * employeeId = -1; EmployeePayrollData employeePayrollData = null; String sql =
     * String.format( "INSERT INTO employee_payroll (name,gender,salary,start)" +
     * "VALUES ('%s','%s','%s','%s')", name, gender, salary,
     * Date.valueOf(startDate)); try (Connection connection = this.getConnection())
     * { Statement statement = connection.createStatement(); int rowAffected =
     * statement.executeUpdate(sql, statement.RETURN_GENERATED_KEYS); if
     * (rowAffected == 1) { ResultSet resultSet = statement.getGeneratedKeys(); if
     * (resultSet.next()) employeeId = resultSet.getInt(1); } employeePayrollData =
     * new EmployeePayrollData(employeeId, dept, name, salary, startDate); } catch
     * (SQLException e) { e.printStackTrace(); } return employeePayrollData; }
     */

    public EmployeePayrollData addEmployeeToPayroll(int dept, String name, double salary, LocalDate startDate,
                                                    String gender) throws SQLException {
        int employeeId = -1;
        Connection connection = null;
        EmployeePayrollData employeePayrollData = null;
        connection = this.getConnection();
        connection.setAutoCommit(false);
        try (Statement statement = connection.createStatement()) {
            String sql = String.format("INSERT INTO employee_payroll (dept,name,gender,salary,start)"
                    + "VALUES ('%s','%s','%s','%s','%s')", dept, name, gender, salary, Date.valueOf(startDate));
            int rowAffected = statement.executeUpdate(sql, statement.RETURN_GENERATED_KEYS);
            if (rowAffected == 1) {
                ResultSet resultSet = statement.getGeneratedKeys();
                if (resultSet.next())
                    employeeId = resultSet.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.rollback();
                return employeePayrollData;
            } catch (SQLException ex) {
                e.printStackTrace();
            }
        }
        try (Statement statement = connection.createStatement()) {
            double deductions = salary * 0.2;
            double taxablePay = salary - deductions;
            double tax = taxablePay * 0.1;
            double netPay = salary - tax;
            String sql = String.format(
                    "INSERT INTO payroll_details (employee_id, basic_pay,deductions,taxable_pay,tax,net_pay) VALUES"
                            + "('%s','%s','%s','%s','%s','%s')",
                    employeeId, salary, deductions, taxablePay, tax, netPay);
            int rowAffected = statement.executeUpdate(sql);
            if (rowAffected == 1) {
                employeePayrollData = new EmployeePayrollData(employeeId, dept, name, salary, startDate);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.rollback();
                return employeePayrollData;
            } catch (SQLException ex) {
                e.printStackTrace();
            }
        }
        try {
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return employeePayrollData;
    }

    public int removeEmployeeFromPayroll(String name) {
        String sql = String.format("update employee_payroll set is_active = 'false' where name = '%s';", name);
        try (Connection connection = this.getConnection()) {
            Statement statement = connection.createStatement();
            return statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
