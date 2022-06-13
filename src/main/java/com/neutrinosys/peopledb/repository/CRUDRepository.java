package com.neutrinosys.peopledb.repository;

import com.neutrinosys.peopledb.model.Entity;

import java.sql.*;

public abstract class CRUDRepository<T extends Entity> {
    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }

    public T save(T entity) {
        try {
            PreparedStatement ps = this.connection.prepareStatement(getSaveSql(), Statement.RETURN_GENERATED_KEYS);
            mapForSave(entity, ps);
            int recordAffected = ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            while (rs.next()) {
                long id = rs.getLong(1);
                // setId het loi vi nho extends Entity, co setId trong do
                entity.setId(id);
                System.out.println(entity);
            }
            System.out.printf("Records affected: %d\n", recordAffected);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return entity;
    }

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

    abstract String getSaveSql();
}

