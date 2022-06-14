package com.neutrinosys.peopledb.repository;

import com.neutrinosys.peopledb.annotation.SQL;
import com.neutrinosys.peopledb.model.Entity;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;

public abstract class CRUDRepository<T extends Entity> {
    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }

    // Reflection techniques / Ctrl + Alt + P is extract parameter for method
    private String getSqlByAnnotation(String methodName, Supplier<String> sqlGetter) {
         return Arrays.stream(this.getClass().getDeclaredMethods())
                // contentEquals or equals its ok, just a little more safe as personal choice. Don't overthink it
                .filter(m -> methodName.contentEquals(m.getName()))
                .map(m -> m.getAnnotation(SQL.class)) // find the method have a type of SQL annotation
                .map(SQL::value)
                .findFirst().orElseGet(sqlGetter); // co the dung orElseGet de reference this method -> this::getSaveSql
    }

    public T save(T entity) {
        try {
            PreparedStatement ps = this.connection.prepareStatement(getSqlByAnnotation("mapForSave", this::getSaveSql), Statement.RETURN_GENERATED_KEYS);
            mapForSave(entity, ps);
            int recordAffected = ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            while (rs.next()) {
                long id = rs.getLong(1);
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

    public List<T> findAll() {
        List<T> entities = new ArrayList<T>();
        try {
            PreparedStatement ps = this.connection.prepareStatement(getFindAllSql());
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                entities.add(extractEntityFromResultSet(rs));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return entities;
    }

    public long count() {
        long result = 0L;
        try {
            PreparedStatement ps = this.connection.prepareStatement(getCountSql());
            ResultSet rs = ps.executeQuery();
            if (rs.next())
                result = rs.getLong(1);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return result;
    }

    public void delete(T entity) {
        try {
            PreparedStatement ps = this.connection.prepareStatement(getDeleteSql());
            ps.setLong(1, entity.getId());
            int affectedRecordCount = ps.executeUpdate();
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void delete(T... entities) {
        String ids = Arrays.stream(entities)
                .map(T::getId)
                .map(String::valueOf)
                .collect(joining(","));
        try {
            Statement stmt = this.connection.createStatement();
            int affectedRecordCount = stmt.executeUpdate(getDeleteInSql().replace(":ids", ids));
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void update(T entity) {
        try {
            PreparedStatement ps = this.connection.prepareStatement(getSqlByAnnotation("mapForUpdate", this::getUpdateSql));
            mapForUpdate(entity, ps);
            ps.setLong(5, entity.getId());
            ps.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    protected String getUpdateSql() { return ""; }

    /**
     *
     * @return Should return a SQL string like:
     * "DELETE FROM PEOPLE WHERE ID IN(:ids)"
     * Be sure to include the '(:ids)' named parameter & call it 'ids'
     */
    protected abstract String getDeleteInSql();
    protected abstract String getDeleteSql();
    protected abstract String getCountSql();
    protected abstract String getFindAllSql();
    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;
    /**
     *
     * @return a String that represents the SQL needed to retrieve one entity
     * The SQL must contain one SQL parameter, i.e. "?", that will bind to the
     * entity's ID.
     */
    protected abstract String getFindByIdSql();
    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;
    abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;
    String getSaveSql() { return ""; }
}

