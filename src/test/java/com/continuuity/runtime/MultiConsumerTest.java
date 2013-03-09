package com.continuuity.runtime;

import com.continuuity.TestHelper;
import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.AbstractGeneratorFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.app.DefaultId;
import com.continuuity.app.guice.BigMamaModule;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.archive.JarFinder;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.SynchronousTransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.discovery.DiscoveryService;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MultiConsumerTest {

  public static final class MultiApp implements Application {

    @Override
    public ApplicationSpecification configure() {
      return ApplicationSpecification.Builder.with()
        .setName("MultiApp")
        .setDescription("MultiApp")
        .noStream()
        .withDataSets().add(new KeyValueTable("accumulated"))
        .withFlows().add(new MultiFlow())
        .noProcedure()
        .build();
    }
  }

  public static final class MultiFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("MultiFlow")
        .setDescription("MultiFlow")
        .withFlowlets()
          .add("gen", new Generator())
          .add("c1", new Consumer())
          .add("c2", new Consumer())
        .connect()
          .from("gen").to("c1")
          .from("gen").to("c2")
        .build();
    }
  }

  public static final class Generator extends AbstractGeneratorFlowlet {

    private OutputEmitter<Integer> output;
    private int i;

    @Override
    public void generate() throws Exception {
      if (i < 100) {
        output.emit(i++);
      }
    }
  }

  private static final byte[] KEY = new byte[] {'k','e','y'};

  public static final class Consumer extends AbstractFlowlet {
    @UseDataSet("accumulated")
    private KeyValueTable accumulated;

    public void process(long l) throws OperationException {
      accumulated.increment(KEY, l);
    }
  }

  @Test
  public void testMulti() throws Exception {
    final CConfiguration configuration = CConfiguration.create();
    configuration.set(Constants.CFG_APP_FABRIC_TEMP_DIR, "/tmp/app/temp");
    configuration.set(Constants.CFG_APP_FABRIC_OUTPUT_DIR, "/tmp/app/archive" + UUID.randomUUID());

    Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules(),
                                             new BigMamaModule(configuration));

    injector.getInstance(DiscoveryService.class).startAndWait();

    LocalLocationFactory lf = new LocalLocationFactory();

    Location deployedJar = lf.create(
      JarFinder.getJar(MultiApp.class, TestHelper.getManifestWithMainClass(MultiApp.class))
    );
    deployedJar.deleteOnExit();

    ListenableFuture<?> p = TestHelper.getLocalManager(configuration).deploy(DefaultId.ACCOUNT, deployedJar);
    ProgramRunnerFactory runnerFactory = injector.getInstance(ProgramRunnerFactory.class);
    final ApplicationWithPrograms app = (ApplicationWithPrograms)p.get();
    List<ProgramController> controllers = Lists.newArrayList();
    for (final Program program : app.getPrograms()) {
      ProgramRunner runner = runnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getProcessorType().name()));
      controllers.add(runner.run(program, new ProgramOptions() {
        @Override
        public String getName() {
          return program.getProgramName();
        }

        @Override
        public Arguments getArguments() {
          return new BasicArguments();
        }

        @Override
        public Arguments getUserArguments() {
          return new BasicArguments();
        }
      }));
    }

    TimeUnit.SECONDS.sleep(2);

    OperationExecutor opex = injector.getInstance(OperationExecutor.class);
    OperationContext opCtx = new OperationContext(DefaultId.ACCOUNT.getId(),
                                                  app.getAppSpecLoc().getSpecification().getName());

    TransactionProxy proxy = new TransactionProxy();
    proxy.setTransactionAgent(new SynchronousTransactionAgent(opex, opCtx));
    DataSetInstantiator dataSetInstantiator = new DataSetInstantiator(new DataFabricImpl(opex, opCtx), proxy,
                                                                      getClass().getClassLoader());
    dataSetInstantiator.setDataSets(ImmutableList.copyOf(new MultiApp().configure().getDataSets().values()));

    KeyValueTable accumulated = dataSetInstantiator.getDataSet("accumulated");
    byte[] value = accumulated.read(KEY);

    Assert.assertEquals(9900L, Longs.fromByteArray(value));

    for (ProgramController controller : controllers) {
      controller.stop().get();
    }
  }
}
