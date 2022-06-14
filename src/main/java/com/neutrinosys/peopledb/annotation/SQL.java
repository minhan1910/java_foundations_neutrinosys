package com.neutrinosys.peopledb.annotation;

import com.neutrinosys.peopledb.model.CrudOperation;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
@Repeatable(MultiSQL.class)
public @interface SQL {
    // Method in interface is default method
    String value();
    CrudOperation operationType();
}
