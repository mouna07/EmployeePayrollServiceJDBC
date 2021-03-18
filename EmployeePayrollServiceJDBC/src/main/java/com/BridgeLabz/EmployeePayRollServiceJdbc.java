package com.BridgeLabz;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;


public class EmployeePayRollServiceJdbc {

    // Enum method to put constants
    public enum IOService {
        CONSOLE_IO, FILE_IO, DB_IO, REST_IO
    }

    // Array list to put employee details
    private List<EmployeePayrollData> employeePayrollList;
    private EmployeePayrollDBService employeePayrollDBService;

    public EmployeePayRollServiceJdbc() {
        employeePayrollDBService = EmployeePayrollDBService.getInstance();
    }

    // Employee pay roll service Constructor
    public EmployeePayRollServiceJdbc(List<EmployeePayrollData> employeePayrollList) {
        this();
        this.employeePayrollList = employeePayrollList;
    }

    // Read method to take employee data from console
    private void readEmployeePayrollData(Scanner consoleInputReader) {
        System.out.print("Enter Employee ID: ");
        int id = consoleInputReader.nextInt();
        System.out.print("Enter Employee Name: ");
        String name = consoleInputReader.next();
        System.out.print("Enter Employee Salary: ");
        double salary = consoleInputReader.nextDouble();
        employeePayrollList.add(new EmployeePayrollData(id, name, salary));
    }

    // Write method to print employee data on console
    public void writeEmployeePayrollData(IOService ioService) {
        if (ioService.equals(IOService.CONSOLE_IO))
            System.out.println("\nWriting Payroll to Console\n" + employeePayrollList);
        else if (ioService.equals(IOService.FILE_IO))
            new EmployeePayrollFileIOService().writeData(employeePayrollList);

    }

    public long countEntries(IOService ioService) {
        // if (ioService.equals(IOService.FILE_IO))
        // return new EmployeePayrollFileIOService().countEntries();
        // System.out.println(employeePayrollList.size());
        return employeePayrollList.size();
    }

    public List<EmployeePayrollData> readEmployeePayrollServiceData(IOService ioService) throws SQLException {
        if (ioService.equals(IOService.DB_IO))
            this.employeePayrollList = employeePayrollDBService.readData();
        return this.employeePayrollList;
    }

    public boolean checkEmployeePayrollInSyncWithDB(String name) {
        List<EmployeePayrollData> employeePayrollDataList = employeePayrollDBService.getEmployeePayrollData(name);
        return employeePayrollDataList.get(0).equals(getEmployeePayrollData(name));
    }

    public void updateEmployeeSalary(String name, double salary) {
        int result = employeePayrollDBService.updateEmployeeData(name, salary);
        if (result == 0)
            return;
        EmployeePayrollData employeePayrollData = this.getEmployeePayrollData(name);
        if (employeePayrollData != null)
            employeePayrollData.salary = salary;
    }

    public  EmployeePayrollData getEmployeePayrollData(String name) {
        return this.employeePayrollList.stream()
                .filter(employeePayrollDataItem -> employeePayrollDataItem.name.equals(name)).findFirst().orElse(null);
    }

    public List<EmployeePayrollData> readEmployeePayrollForDateRange(IOService ioService, LocalDate startDate,
                                                                     LocalDate endDate) {
        if (ioService.equals(IOService.DB_IO))
            return employeePayrollDBService.getEmployeePayrollForDateRange(startDate, endDate);
        return null;
    }

    public Map<String, Double> readAverageSalaryByGender(IOService ioService) {
        if (ioService.equals(IOService.DB_IO))
            return employeePayrollDBService.getAverageSalaryByGender();
        return null;
    }

    public Map<String, Double> readSumOfSalaryByGender(IOService ioService) {
        if (ioService.equals(IOService.DB_IO))
            return employeePayrollDBService.getSumOfSalaryByGender();
        return null;
    }

    public Map<String, Double> readCountOfSalaryByGender(IOService ioService) {
        if (ioService.equals(IOService.DB_IO))
            return employeePayrollDBService.getCountOfSalaryByGender();
        return null;
    }

    public Map<String, Double> readMaxOfSalaryByGender(IOService ioService) {
        if (ioService.equals(IOService.DB_IO))
            return employeePayrollDBService.getMaxOfSalaryByGender();
        return null;
    }

    public Map<String, Double> readMinOfSalaryByGender(IOService ioService) {
        if (ioService.equals(IOService.DB_IO))
            return employeePayrollDBService.getMinOfSalaryByGender();
        return null;
    }

    public void addEmployeeToPayroll(int dept, String name, double salary, LocalDate startDate, String gender)
            throws SQLException {
        employeePayrollList.add(employeePayrollDBService.addEmployeeToPayroll(dept, name, salary, startDate, gender));
    }

    public void removeEmployeeFromPayrollService(String name) {
        employeePayrollDBService.removeEmployeeFromPayroll(name);
    }

    public void addEmployeesToPayroll(List<EmployeePayrollData> employeePayrollDataList) {
        employeePayrollDataList.forEach(employeePayrollData -> {
            System.out.println("Employee Being Added: " + employeePayrollData.name);
            try {
                this.addEmployeeToPayroll(employeePayrollData.dept, employeePayrollData.name,
                        employeePayrollData.salary, employeePayrollData.startDate, employeePayrollData.gender);
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            System.out.println("Employee Added: " + employeePayrollData.name);
        });
        System.out.println(this.employeePayrollList);
    }

    public void addEmployeesToPayrollWithThreads(List<EmployeePayrollData> employeePayrollDataList) {
        Map<Integer, Boolean> employeeAdditionStatus = new HashMap<Integer, Boolean>();
        employeePayrollDataList.forEach(employeePayrollData -> {
            Runnable task = () -> {
                employeeAdditionStatus.put(employeePayrollData.hashCode(), false);
                System.out.println("Employee Being Added: " + Thread.currentThread().getName());
                try {
                    this.addEmployeeToPayroll(employeePayrollData.dept, employeePayrollData.name,
                            employeePayrollData.salary, employeePayrollData.startDate, employeePayrollData.gender);
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                employeeAdditionStatus.put(employeePayrollData.hashCode(), false);
                System.out.println("Employee Added: " + employeePayrollData.name);
            };
            Thread thread = new Thread(task, employeePayrollData.name);
            thread.start();
        });
        while (employeeAdditionStatus.containsValue(false)) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(this.employeePayrollList);

    }

}
