#include <stdio.h>
#include <stdlib.h>

#include "src/ydb_db.h"
#include "src/ydb_write.h"
#include "src/ydb-internal.h"
#include "ft/ft.h"
#include "ft/txn/txn.h"
#include "ft/logger/log-internal.h"
#include "toku_assert.h"

#include <vector>
#include <string>

DB_ENV *dbenv;
const char *homedir = "/home/raw/tokudb-debug/percona-server-5.6-install/data/";

struct dname_iname_pair {
    std::string dname;
    std::string iname;
};

typedef std::vector<dname_iname_pair> dir_pairs;
std::string get_db_name(const std::string &dname);

std::string get_db_name(const std::string &dname) {
    std::string result;
    auto ptr = dname.begin();

    assert(dname.size() >= 2);
    assert(*ptr++ == '.' && *ptr++ == '/');

    for (auto end = dname.end(); ptr != end; ++ptr)
        if (*ptr == '/') {
          result.assign(dname.begin() + 2, ptr);
          break;
        }

    return result;
};

int main(void) {
    int r;
    int flags;
    DB_TXN *txn = NULL;
    DBC* dir_cursor = NULL;
    DBT curr_key;
    DBT curr_val;
    dir_pairs dps;

    memset(&curr_key, 0, sizeof curr_key);
    memset(&curr_val, 0, sizeof curr_val);

    r = db_env_create(&dbenv, 0);
    assert_zero(r);

    dbenv->set_errpfx(dbenv, "converter");

    flags = DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_MPOOL | DB_INIT_TXN | DB_RECOVER | DB_PRIVATE | DB_CREATE;

    r = dbenv->open(dbenv, homedir, flags, 0);
    assert_zero(r);

    r = dbenv->txn_begin(dbenv, 0, &txn, DB_READ_UNCOMMITTED);
    assert_zero(r);

    r = dbenv->get_cursor_for_directory(dbenv, txn, &dir_cursor);
    assert_zero(r);

    while (r == 0) {
        r = dir_cursor->c_get(dir_cursor, &curr_key, &curr_val, DB_NEXT);
        if (!r) {
            const char *dname = (const char *) curr_key.data;
            const char *iname = (const char *) curr_val.data;
            dps.push_back({dname, iname});
        //    printf("%s - %s\n", dname, iname);

        }
    }

    if (dir_cursor) {
        r = dir_cursor->c_close(dir_cursor);
        assert_zero(r);
    }

    if (txn) {
        r = txn->commit(txn, 0);
        assert_zero(r);
    }

    std::string dbdir(dbenv->i->dir);


    for (auto &i : dps) {
        toku_struct_stat stat;

        std::string db_name = get_db_name(i.dname);
        std::string new_iname = "./" + db_name + "/" + (i.iname.c_str() + 2 + db_name.size() + 2);
        std::string iname_path = dbdir + (i.iname.c_str() + 2);
        std::string db_name_path = dbdir + db_name;
        std::string new_iname_path = dbdir + (new_iname.c_str() + 2);

        if (toku_stat(db_name_path.c_str(), &stat) == -1) {
            if(ENOENT == errno) {
                if (toku_os_mkdir(db_name_path.c_str(), 0777)) {
                    fprintf(stderr, "mkdir error: %s\n", db_name_path.c_str());
                    return EXIT_FAILURE;
                }
            } else {
                fprintf(stderr, "stat error\n");
                return EXIT_FAILURE;
            }
        }


        BYTESTRING bt_dname = {static_cast<uint32_t>(i.dname.size()) + 1, const_cast<char *>(i.dname.c_str())};
        BYTESTRING bt_old_iname = {static_cast<uint32_t>(i.iname.size()) + 1, const_cast<char *>(i.iname.c_str())};
        BYTESTRING bt_new_iname = {static_cast<uint32_t>(new_iname.size()) + 1, const_cast<char *>(new_iname.c_str())};

        r = dbenv->txn_begin(dbenv, 0, &txn, DB_READ_UNCOMMITTED);
        assert_zero(r);

        TOKUTXN tokutxn = db_txn_struct_i(txn)->tokutxn;

        // make entry in rollback log
        toku_logger_save_rollback_imove(tokutxn, &bt_dname, &bt_old_iname, &bt_new_iname);
        // make entry in recovery log
        toku_logger_log_imove(tokutxn, bt_dname, bt_old_iname, bt_new_iname);


        if (rename(iname_path.c_str(), new_iname_path.c_str()) == -1) {
            fprintf(stderr, "rename error, can not rename from %s to %s\n", iname_path.c_str(), new_iname_path.c_str());
            return EXIT_FAILURE;
        }
//toku_construct_full_name
        toku_fill_dbt(&curr_key, i.dname.c_str(), i.dname.size() + 1);
        toku_fill_dbt(&curr_val, new_iname.c_str(), new_iname.size() + 1);

        toku_db_put(dbenv->i->directory, txn, &curr_key, &curr_val, 0, true);
        //r = toku_db_update(dbenv->i->directory, txn, &curr_key, &curr_val, 0);
        assert_zero(r);

//        if (unlink(iname_path.c_str()) == -1) {
//            fprintf(stderr, "link error\n");
//            return EXIT_FAILURE;
//        }

        if (txn) {
            r = txn->commit(txn, 0);
        //    txn->abort(txn);
            assert_zero(r);
       }

        //printf("%s - %s\n", i.dname.c_str(), db_name.c_str());
    }


    dbenv->close(dbenv, 0);
    return EXIT_SUCCESS;
}
