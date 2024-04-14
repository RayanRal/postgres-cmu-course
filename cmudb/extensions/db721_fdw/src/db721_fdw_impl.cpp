// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

#include "dog.h"

// clang-format off
extern "C" {
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
#include "../../../../src/include/foreign/fdwapi.h"
#include "commands/defrem.h"
#include "optimizer/pathnode.h"
#include "../../../../src/include/foreign/foreign.h"
#include <stdio.h>
#include <stdlib.h>
}
// clang-format on

#define MAX_TABLE_NAME_LENGTH 100
#define MAX_COLUMN_NAME_LENGTH 100
#define MAX_METADATA_SIZE 1024


struct DB721FdwPlanState
{
    List       *filenames;
    //List       *attrs_sorted;
    //Bitmapset  *attrs_used;     /* attributes actually used in query */
    //bool        use_mmap;
   // bool        use_threads;
    //int32       max_open_files;
    //bool        files_in_order;
    //List       *rowgroups;      /* List of Lists (per filename) */
    uint64      matched_rows;
    //ReaderType  type;
};

struct DB721Metadata
        {
    char table_name[MAX_TABLE_NAME_LENGTH];
    List *columns;
    int max_values_per_block;

};

static void get_foreign_table(Oid foreigntableid, DB721FdwPlanState *fdw_private) {
    elog(LOG, "In get_foreign_table");
    ForeignTable *table;
    ListCell     *lc;
    table = GetForeignTable(foreigntableid);
    List       *filenames = NIL;
    foreach(lc, table->options)
    {
        DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "filename") == 0)
        {
            elog(LOG, "Got filenames '%s'", defGetString(def));
            filenames = lappend(filenames, defGetString(def));
        }
        else
            elog(LOG, "unknown option '%s'", def->defname);
    }
    fdw_private->filenames = filenames;
}

static void read_file_metadata(DB721FdwPlanState *fdw_private) {
    elog(LOG, "In read_file_metadata");
    FILE *file;
    unsigned int metadata_size;
    char metadata[MAX_METADATA_SIZE];

    file = fopen("/Users/leonidchashnikov/Projects/postgres-cmu-course/test-data/data-farms.db721", "rb");
    if (file == NULL) {
        elog(ERROR, "Error opening file");
        return;
    }
    fseek(file, -4, SEEK_END); // Seek to the beginning of the last 4 bytes
    fread(&metadata_size, sizeof(unsigned int), 1, file); // Read the metadata size
    fseek(file, -(int) metadata_size - 4, SEEK_END);     // Seek to the beginning of the metadata
    fread(metadata, 1, metadata_size, file);
    metadata[metadata_size] = '\0';
    elog(LOG, "Metadata: %s", metadata);
    fclose(file);
}

extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreigntableid) {
  // update baserel->rows
    DB721FdwPlanState *db721FdwPlanState;
    db721FdwPlanState = (DB721FdwPlanState *) palloc0(sizeof(DB721FdwPlanState));
    get_foreign_table(foreigntableid, db721FdwPlanState);
    elog(LOG, "finished get_foreign_table");
    read_file_metadata(db721FdwPlanState);
  // baserel->rows = 2;
  Dog terrier("Terrier");
    //elog(LOG, "db721_GetForeignRelSize: %s", );
   elog(LOG, "db721_GetForeignRelSize: %s", terrier.Bark().c_str());
}

extern "C" void db721_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                                    Oid foreigntableid) {
  // TODO(721): Write me!
  // generate at least one access path (ForeignPath node) for a scan on the foreign table
  // and must call add_path to add each such path to baserel->pathlist
   // It's recommended to use create_foreignscan_path to build the ForeignPath nodes
   //ForeignPath path = create_foreignscan_path(root, baserel, , baserel->rows)
   // add_path(path)
  Dog scout("Scout");
  elog(LOG, "db721_GetForeignPaths: %s", scout.Bark().c_str());
}

extern "C" ForeignScan *
db721_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
                   ForeignPath *best_path, List *tlist, List *scan_clauses,
                   Plan *outer_plan) {
    Dog foreigner("Foreigner");
    elog(LOG, "db721_GetForeignPlan: %s", foreigner.Bark().c_str());
  // TODO(721): Write me!
  return nullptr;
}

extern "C" void db721_BeginForeignScan(ForeignScanState *node, int eflags) {
  // TODO(721): Write me!
}

extern "C" TupleTableSlot *db721_IterateForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
  return nullptr;
}

extern "C" void db721_ReScanForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
}

extern "C" void db721_EndForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
}

