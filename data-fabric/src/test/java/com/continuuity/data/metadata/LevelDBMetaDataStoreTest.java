package com.continuuity.data.metadata;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.engine.leveldb.LevelDBOVCTableHandle;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

import static org.junit.Assert.assertTrue;

/**
 * LevelDB backed metadata store tests.
 */
public abstract class LevelDBMetaDataStoreTest extends MetaDataStoreTest {

  protected static Injector injector;

  @BeforeClass
  public static void setupDataFabric() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.unset(Constants.CFG_DATA_LEVELDB_DIR);
    injector = Guice.createInjector(new DataFabricLevelDBModule(conf));
  }
}
