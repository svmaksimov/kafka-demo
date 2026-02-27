package com.example.model;

public class User {
    public String id;
    public String name;
    public String email;
    public String department;

    // Пустой конструктор для Jackson
    public User() {}

    @Override
    public String toString() {
        return "User{id='" + id + "', name='" + name + "', dept='" + department + "'}";
    }
}
