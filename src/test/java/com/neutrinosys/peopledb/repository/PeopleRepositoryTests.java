package com.neutrinosys.peopledb.repository;

import com.neutrinosys.peopledb.model.Person;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTests {
    private PeopleRepository repo;
    private Connection connection;
    @BeforeEach
    void setUp() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            //.replace("~", System.getProperty("user.home"))
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/peopletest", "root", "");
            this.repo = new PeopleRepository(connection);
        } catch (SQLException | ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void canSaveOnePerson() {
        Person john = new Person("An", "Smith", ZonedDateTime.of(2002, 11, 19, 15, 15, 0, 0, ZoneId.of("-6")));
        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSaveTwoPeople() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")));
        Person bobby = new Person("Bobby", "Smith", ZonedDateTime.of(1982, 9, 13, 13, 13, 0, 0, ZoneId.of("-8")));
        Person savedPerson1 = repo.save(john);
        Person savedPerson2 = repo.save(bobby);
        assertThat(savedPerson1.getId()).isNotEqualTo(savedPerson2.getId());
    }

    /**
     * This test not match by DOB field
     */
    @Test
    public void canFindPersonById() {
        Person savedPerson = this.repo.save(new Person("test", "jackson", ZonedDateTime.now()));
        Person foundPerson = this.repo.findById(savedPerson.getId()).get(); // .get by Optional
        assertThat(foundPerson).isEqualTo(savedPerson);
    }

    @Test
    public void testPersonIdNotFound() {
        Optional<Person> foundPerson = this.repo.findById(-1L);
        assertThat(foundPerson).isEmpty();
    }

    @Test
    public void canGetAllPeople() {
        List<Person> people = this.repo.findAll();
        assertThat(people).hasSizeGreaterThan(10);
    }

    @Test
    public void canGetCount() {
        long startCount = this.repo.count();
        this.repo.save(new Person("test", "jackson", ZonedDateTime.now()));
        this.repo.save(new Person("test", "jackson", ZonedDateTime.now()));
        long endCount = this.repo.count();
        assertThat(endCount).isEqualTo(startCount + 2);
    }

    @Test
    public void canDelete() {
        Person savedPerson = this.repo.save(new Person("test", "jackson", ZonedDateTime.now()));
        long startCount = this.repo.count();
        this.repo.delete(savedPerson);
        long endCount = this.repo.count();
        assertThat(endCount).isEqualTo(startCount - 1);
    }

    @Test
    public void canDeleteMultiplePeople() {
        Person p1 = this.repo.save(new Person("test", "jackson", ZonedDateTime.now()));
        Person p2 = this.repo.save(new Person("test", "jackson", ZonedDateTime.now()));

        long startCount = this.repo.count();
        this.repo.delete(p1, p2);
        long endCount = this.repo.count();
        assertThat(endCount).isEqualTo(startCount - 2);
    }

    @Test
    public void canUpdate() {
        Person savedPerson = this.repo.save(new Person("test", "jackson", ZonedDateTime.now()));

        // capture the old savedPerson which is not updated the salary field
        Person foundP = this.repo.findById(savedPerson.getId()).get();// get will return defined person

        savedPerson.setSalary(new BigDecimal(73000.34));
        this.repo.update(savedPerson);

        Person foundP2 = this.repo.findById(savedPerson.getId()).get();

        assertThat(foundP2.getSalary()).isNotEqualTo(foundP.getSalary());
    }

    @Test
    public void experiment() {
        Person p1 = new Person(28L, null, null, null);
        Person p2 = new Person(35L, null, null, null);
        Person p3 = new Person(29L, null, null, null);
        Person p4 = new Person(40L, null, null, null);
        Person p5 = new Person(31L, null, null, null);

        // DELETE FROM PERSON WHERE ID (10, 20, 30, 40, 50);

        // toArray take argument with type Person not Object
        Person[] people = Arrays.asList(p1, p2, p3, p4, p5).toArray(new Person[]{});
//        this.repo.delete(people);
    }
}

