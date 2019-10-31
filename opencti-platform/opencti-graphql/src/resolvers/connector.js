import {
  connectors,
  registerConnector,
  pingConnector,
  connectorsForExport,
  connectorsForImport
} from '../domain/connector';
import {
  connectorForWork,
  jobsForWork,
  updateJob,
  initiateJob,
  deleteWork,
  computeWorkStatus,
  workForConnector
} from '../domain/work';

const connectorResolvers = {
  Query: {
    connectors: () => connectors(),
    connectorsForExport: () => connectorsForExport(),
    connectorsForImport: () => connectorsForImport()
  },
  Connector: {
    works: connector => workForConnector(connector.id)
  },
  Work: {
    jobs: work => jobsForWork(work.id),
    status: work => computeWorkStatus(work.id),
    connector: work => connectorForWork(work.id)
  },
  Mutation: {
    registerConnector: (_, { input }) => registerConnector(input),
    pingConnector: (_, { id, state }) => pingConnector(id, state),
    initiateJob: (_, { workId }) => initiateJob(workId),
    updateJob: (_, { jobId, status, messages }) =>
      updateJob(jobId, status, messages),
    deleteWork: (_, { id }) => deleteWork(id)
  }
};

export default connectorResolvers;
