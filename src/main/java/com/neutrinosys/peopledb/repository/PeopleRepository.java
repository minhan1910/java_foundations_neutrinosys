package com.neutrinosys.peopledb.repository;

import com.neutrinosys.peopledb.model.Person;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

public class PeopleRepository extends CRUDRepository<Person> {
    public static final String FIND_BY_ID_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE WHERE ID = ?";
    public static final String SAVE_PERSON_SQL = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES (?, ?, ?)";
    public static final String FIND_ALL_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE";
    private Connection connection;

    public PeopleRepository(Connection connection) {
        super(connection);
    }

    @Override
    String getSaveSql() {
        return PeopleRepository.SAVE_PERSON_SQL;
    }

    @Override
    void mapForSave( Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, this.convertDobToTimestamp(entity.getDob()));
    }

    @Override
    Person extractEntityFromResultSet(ResultSet rs) throws SQLException {
        long personId = rs.getLong("ID");
        String firstName = rs.getString("FIRST_NAME");
        String lastName = rs.getString("LAST_NAME");
        ZonedDateTime dob = ZonedDateTime.of(rs.getTimestamp("DOB").toLocalDateTime(), ZoneId.of("+0"));// +0 -> work every time zone
        BigDecimal salary = rs.getBigDecimal("SALARY");
        return new Person(personId, firstName, lastName, dob, salary);
    }

    @Override
    protected String getFindByIdSql() {
        return FIND_BY_ID_SQL;
    }

    //    // Ctrl + Alt + C -> extract string into const of  member class
//    public Person save(Person person) {
//        String sql = String.format(SAVE_PERSON_SQL);
//        try {
//            PreparedStatement ps = this.connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
////            mapForSave(ps, person, convertDobToTimestamp(person.getDob()));
//            int recordAffected = ps.executeUpdate();
//            ResultSet rs = ps.getGeneratedKeys();
//            while (rs.next()) {
//                long id = rs.getLong(1);
//                person.setId(id);
//            }
//            System.out.printf("Records affected: %d\n", recordAffected);
//        } catch (SQLException e) {
//            System.out.println(e.getMessage());
//        }
//        return person;
//    }

    public Optional<Person> findById(Long id) {
        Person person = null;
        try {
            PreparedStatement ps = super.connection.prepareStatement(FIND_BY_ID_SQL);
            ps.setLong(1, id);
            ResultSet rs = ps.executeQuery();// for query
            while (rs.next()) {
                person = extractEntityFromResultSet(rs);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return Optional.ofNullable(person);
    }

    public long count() {
        long result = 0L;
        try {
            PreparedStatement ps = super.connection.prepareStatement("SELECT COUNT(*) FROM PEOPLE");
            ResultSet rs = ps.executeQuery();
            while(rs.next())
                result = rs.getLong(1);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return result;
    }

    public List<Person> findAll() {
        List<Person> result = new ArrayList<Person>();
        try {
            PreparedStatement ps = super.connection.prepareStatement(FIND_ALL_SQL);
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                Long id = rs.getLong("ID");
                String firstName = rs.getString("FIRST_NAME");
                String lastName = rs.getString("LAST_NAME");
                ZonedDateTime dob = ZonedDateTime.ofInstant(rs.getTimestamp("DOB").toInstant(), ZoneId.of("+0"));
                Person newPerson = new Person(firstName, lastName, dob);
                newPerson.setId(id);
                result.add(newPerson);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return result;
    }

    public void delete(Person person) {
        try {
            PreparedStatement ps = super.connection.prepareStatement("DELETE FROM PEOPLE WHERE ID = ?");
            ps.setLong(1, person.getId());
            int affectedRecordCount = ps.executeUpdate();
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

    }

    public void delete(Person... people) {
        // Handling with imperative programming
//        StringBuilder ids = new StringBuilder(people.length);
//        String prefix = "";
//        for (Person person : people) {
//            ids.append(prefix);
//            prefix = ",";
//            ids.append(person.getId().toString());
//        }

        // with funional programming
        String ids = Arrays.stream(people)
                .map(Person::getId)
                .map(String::valueOf)
                .collect(joining(","));
        try {
            Statement stmt = super.connection.createStatement();
            int affectedRecordCount = stmt.executeUpdate("DELETE FROM PEOPLE WHERE ID IN(:ids)".replace(":ids", ids));
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void update(Person person) {
        try {
            PreparedStatement ps = super.connection.prepareStatement("UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=? WHERE ID=?");
            ps.setString(1, person.getFirstName());
            ps.setString(2, person.getLastName());
            ps.setTimestamp(3, this.convertDobToTimestamp(person.getDob()));
            ps.setBigDecimal(4, person.getSalary());
            ps.setLong(5, person.getId());
            ps.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    // Ctrl + Alt + P no se doi tu person.dob -> ZonedDateTime dob
    private Timestamp convertDobToTimestamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }

}
