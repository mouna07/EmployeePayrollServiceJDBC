package com.BridgeLabz;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Objects;

public class EmployeePayrollData {

    public int id;
    public String name;
    public double salary;
    public LocalDate startDate;
    public int dept;
    public String gender;

    // Constructor for employee data
    public EmployeePayrollData(Integer id, String name, Double salary) {
        this.id = id;
        this.name = name;
        this.salary = salary;
    }

    // Constructor to add StartDate
    public EmployeePayrollData(int id, int dept, String name, double salary, LocalDate startDate) {
        this(id, name, salary);
        this.dept = dept;
        this.startDate = startDate;
    }

    public EmployeePayrollData(int id,  int dept, String name, String gender, double salary, LocalDate startDate) {
        this(id, dept,name, salary, startDate);
        this.gender = gender;
    }

    // ToString method declaration
    @Override
    public String toString() {
        return "EmployeePayrollData [id=" + id + ", name=" + name + ", salary=" + salary + ", startDate=" + startDate
                + ", dept=" + dept + "]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(dept,name,salary,startDate);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EmployeePayrollData other = (EmployeePayrollData) obj;
        if (dept != other.dept)
            return false;
        if (id != other.id)
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (Double.doubleToLongBits(salary) != Double.doubleToLongBits(other.salary))
            return false;
        if (startDate == null) {
            if (other.startDate != null)
                return false;
        } else if (!startDate.equals(other.startDate))
            return false;
        return true;
    }


}
