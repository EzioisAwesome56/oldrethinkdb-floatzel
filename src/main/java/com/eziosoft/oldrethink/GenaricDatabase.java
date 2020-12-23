package com.eziosoft.oldrethink;

public interface GenaricDatabase {

    void Conninfo(String info);

    String getProfile(String id);

    void saveProfile(String json);

    void initDatabase();

    boolean checkForUser(String id);
}
