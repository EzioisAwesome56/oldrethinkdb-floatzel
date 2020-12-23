package com.eziosoft.oldrethink;

import com.eziosoft.floatzel.Objects.GenaricDatabase;
import com.google.gson.Gson;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.exc.ReqlError;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;

import java.util.List;

public class DbCode implements GenaricDatabase {

    private static final RethinkDB r = RethinkDB.r;
    private static Connection thonk;
    private static Gson gson = new Gson();
    private ConnInfo info;

    // direct carryover from floatzel's codebase
    private static String banktable = "bank";
    private static String loantable = "loan";
    private static String bloanperm = "bloan";
    private static String stocktable = "stocks";
    private static String stockbuy = "boughtstock";
    private static String tweets = "tweets";
    private static String tagperm = "gtagperm";
    private static String tags = "tags";
    private static String ass = "ass";

    // create all the tables!
    private static void makeTables(){
        // run a bunch of rethink commands
        r.tableCreate(banktable).run(thonk);
        r.tableCreate(loantable).run(thonk);
        r.tableCreate(bloanperm).run(thonk);
        r.tableCreate(stocktable).run(thonk);
        r.tableCreate(tweets).run(thonk);
        r.tableCreate(tagperm).run(thonk);
        r.tableCreate(tags).run(thonk);
        r.tableCreate(stockbuy).run(thonk);
        r.tableCreate(ass).run(thonk);
    }


    @Override
    public void Conninfo(String info) {
        this.info = gson.fromJson(info, ConnInfo.class);
    }

    @Override
    public String getProfile(String id) {
        // this is gonna be one hell of a clusterfuck
        int stockid = -1;
        if (checkBought(id)){
            stockid = getBoughtId(id);
        }
        User h = new User(
                id,
                loadBal(id),
                getLoanTime(id),
                new Boolean[]{checkPermission(id), false},
                stockid,
                false
        );
        return gson.toJson(h);
    }

    @Override
    public void saveProfile(String json) {
        // first get user profile
        User h = gson.fromJson(json, User.class);
        // then run the many, many, MANY functions to save the profile
        saveBal(h.getUid(), h.getBal());
        saveLoanTime(h.getUid(), h.getLastloan());
        if (h.getPerms()[0]){
            if (!checkPermission(h.getUid())){
                setPermission(h.getUid());
            }
        }
        if (h.getStockid() == -1){
            deleteUserStock(h.getUid());
        } else {
            buyStock(h.getUid(), h.getStockid());
        }


    }

    private void saveBal(String id, int bal){
        try {
            r.table(banktable).filter(row -> row.g("uid").eq(id)).update(r.hashMap("bal", bal)).run(thonk);
        } catch (ReqlError e){
            e.printStackTrace();
        }
    }

    private int loadBal(String id){
        Cursor h = r.table(banktable).filter(row -> row.g("uid").eq(id)).getField("bal").run(thonk);
        return Integer.valueOf(getValue(h));
    }

    private Long getLoanTime(String uid){
        Cursor cur = null;
        // first we check if they even have an entry in the table
        if (!(boolean)r.table(loantable).filter(r.hashMap("uid", uid)).count().eq(1).run(thonk)){
            // just save them a 0 and return 0
            r.table(loantable).insert(r.array(r.hashMap("uid", uid).with("time", Long.toString(0L)))).run(thonk);
            return 0L;
        } else {
            cur = r.table(loantable).filter(row -> row.g("uid").eq(uid)).getField("time").run(thonk);
            return Long.valueOf(getValue(cur));
        }
    }

    private void saveLoanTime(String id, long time){
        r.table(loantable).filter(row -> row.g("uid").eq(id)).update(r.hashMap("time", Long.toString(time))).run(thonk);
    }

    private void setPermission(String uid){
        r.table(bloanperm).insert(r.hashMap("uid", uid)).run(thonk);
    }

    private boolean checkPermission(String uid){
        return (boolean) r.table(bloanperm).filter(row -> row.g("uid").eq(uid)).count().eq(1).run(thonk);
    }

    private boolean checkBought(String uid){
        return (boolean) r.table(stockbuy).filter(row -> row.g("uid").eq(uid)).count().eq(1).run(thonk);
    }

    private void buyStock(String uid, int id){
        r.table(stockbuy).insert(r.hashMap("uid", uid).with("sid", id)).run(thonk);
    }

    private void deleteUserStock(String id){
        r.table(stockbuy).filter(row -> row.g("uid").eq(id)).delete().run(thonk);
    }

    private int getBoughtId(String id){
        Cursor cur = null;
        cur = r.table(stockbuy).filter(row -> row.g("uid").eq(id)).getField("sid").run(thonk);
        return Integer.valueOf(getValue(cur));
    }

    @Override
    public void initDatabase() {
        // lifted straight from floatzel 2.5.6.4's codebase, with minor tweaks
        Connection.Builder builder = r.connection().hostname("localhost").port(28015);
        if (!info.getUser().equals("null")) {
            builder.user(info.getUser(), info.getPass().equals("null") ? "" : info.getPass());
        } else {
            builder.user("admin", info.getPass().equals("null") ? "" : info.getPass());
        }

        thonk = builder.connect();
        System.out.println("Eziosoft RethinkDB Driver v1.0 now starting up...");
        // first, check if the database file exists
        if (!(boolean) r.dbList().contains("floatzel").run(thonk)){
            System.out.println("No database found! Creating a new db!");
            // okay, it hasnt been initalized yet, so do that
            r.dbCreate("floatzel").run(thonk);
            thonk.use("floatzel");
            System.out.println("Creating tables...");
            makeTables();
        } else {
            // set the default db for rethonk
            thonk.use("floatzel");
            System.out.println("ReThinkDB started!");
        }
        return;
    }

    // yanked from floatzel utils
    private static String getValue(Cursor cur) throws IndexOutOfBoundsException{
        try {
            List curlist = cur.toList();
            String value = curlist.get(0).toString();
            return value;
        } catch (IndexOutOfBoundsException e){
            throw e;
        }
    }

    @Override
    public boolean checkForUser(String id) {
        // lifted from 2.5.6.4
        boolean exist = false;
        // connection shit
        try {
            exist = (boolean) r.table(banktable).filter(
                    r.hashMap("uid", id)
            ).count().eq(1).run(thonk);
        } catch (ReqlError e){
            e.printStackTrace();
            return false;
        }
        if (!exist){
            // the user does not have a bank account
            // make one instead!
            try {
                r.table(banktable).insert(r.array(
                        r.hashMap("uid", id)
                                .with("bal", 0)
                )).run(thonk);
            } catch (ReqlError e){
                e.printStackTrace();
                return false;
            }
            return exist;
        } else {
            return exist;
        }
    }

    @Override
    public int totalStocks() {
        return Math.toIntExact(r.table(stocktable).count().run(thonk));
    }

    @Override
    public void makeNewStock(String s) {
        Stock w = gson.fromJson(s, Stock.class);
        r.table(stocktable).insert(r.array(r.hashMap("sid", w.getId()).with("name", w.getName()).with("units", w.getUnits()).with("price", w.getPrice()).with("diff", 0))).run(thonk);
    }

    @Override
    public void updateStock(String s) {
        Stock h = gson.fromJson(s, Stock.class);
        int old = getStockPrice(h.getId());
        r.table(stocktable).filter(row -> row.g("sid").eq(h.getId())).update(
                r.hashMap("price", h.getPrice())
        ).run(thonk);
        r.table(stocktable).filter(row -> row.g("sid").eq(h.getId())).update(
                r.hashMap("diff", h.getDiff())
        ).run(thonk);
        r.table(stocktable).filter(row -> row.g("sid").eq(h.getId())).update(
                r.hashMap("units", h.getUnits())
        ).run(thonk);
    }

    @Override
    public String getStock(int id) {
        Stock h = new Stock(
                id,
                getStockName(id),
                getStockUnits(id),
                getStockPrice(id),
                getStockDiff(id)
        );
        return gson.toJson(h);
    }

    private int getStockPrice(int id){
        Cursor h = r.table(stocktable).filter(row -> row.g("sid").eq(id)).getField("price").run(thonk);
        return Integer.valueOf(getValue(h));
    }

    private String getStockName(int id){
        Cursor h = r.table(stocktable).filter(row -> row.g("sid").eq(id)).getField("name").run(thonk);
        return getValue(h);
    }

    private int getStockUnits(int id){
        Cursor h = r.table(stocktable).filter(row -> row.g("sid").eq(id)).getField("units").run(thonk);
        return Integer.valueOf(getValue(h));
    }

    private int getStockDiff(int id){
        Cursor h = r.table(stocktable).filter(row -> row.g("sid").eq(id)).getField("diff").run(thonk);
        return Integer.valueOf(getValue(h));
    }
}
