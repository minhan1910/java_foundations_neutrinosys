package com.neutrinosys.peopledb.repository;

import com.neutrinosys.peopledb.model.Entity;
import com.neutrinosys.peopledb.model.Person;

import java.sql.*;
import java.util.Optional;

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

    public Optional<T> findById(Long id) {
        T entity = null;
        try {
            PreparedStatement ps = this.connection.prepareStatement(getFindByIdSql());
            ps.setLong(1, id);
            ResultSet rs = ps.executeQuery();// for query
            while (rs.next()) {
                entity = extractEntityFromResultSet(rs);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return Optional.ofNullable(entity);
    }

    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;

    /**
     *
     * @return a String that represents the SQL needed to retrieve one entity
     * The SQL must contain one SQL parameter, i.e. "?", that will bind to the
     * entity's ID.
     */
    protected abstract String getFindByIdSql();

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

    abstract String getSaveSql();
}

