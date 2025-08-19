import com.milvus.common.TSO;
import com.milvus.coordinator.root.RootCoordinatorServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataCoorDemo {
    private static final Logger logger = LoggerFactory.getLogger(DataCoorDemo.class);

    public static void main(String[] args){
        RootCoordinatorServer rootCoordinator = new RootCoordinatorServer("root-coordinator", 7000, "http://localhost:2379");
        rootCoordinator.start();

        TSO tso = rootCoordinator.allocateTimestamp();
        logger.info("Allocated timestamp: {}", tso);


    }
}
