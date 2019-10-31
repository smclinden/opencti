import uuid from 'uuid/v4';
import { assoc, filter, map, pipe, reduce, add } from 'ramda';
import moment from 'moment';
import { getById, now, sinceNowInMinutes } from '../database/grakn';
import {
  el,
  elDeleteByField,
  getAttributes,
  index,
  paginate
} from '../database/elasticSearch';

export const WORK_INDEX = 'connector_works';

export const workToExportFile = work => {
  return {
    id: work.internal_id_key,
    name: work.work_file,
    size: 0,
    lastModified: moment(work.updated_at).toDate(),
    lastModifiedSinceMin: sinceNowInMinutes(work.updated_at),
    uploadStatus: 'progress',
    metaData: {
      category: 'export'
    }
  };
};

export const connectorForWork = async id => {
  const work = await getAttributes(WORK_INDEX, id);
  if (work) return getById(work.connector_id);
  return null;
};

export const jobsForWork = async id => {
  return paginate(WORK_INDEX, {
    type: 'Job',
    connectionFormat: false,
    orderBy: 'created_at',
    orderMode: 'asc',
    filters: {
      work_id: id
    }
  });
};

const computeState = async (totalCount, stats) => {
  // Compute in progress
  const isProgress = st => st.status === 'wait' || st.status === 'progress';
  const nbProgress = pipe(
    filter(stat => isProgress(stat)),
    map(e => e.count),
    reduce(add, 0)
  )(stats);
  if (nbProgress > 0) return 'progress';
  // Is fully error
  const nbErrors = pipe(
    filter(stat => stat.status === 'error'),
    map(e => e.count),
    reduce(add, 0)
  )(stats);
  if (nbErrors === totalCount) return 'error';
  // Is fully complete
  const nbComplete = pipe(
    filter(stat => stat.status === 'complete'),
    map(e => e.count),
    reduce(add, 0)
  )(stats);
  if (nbComplete === totalCount) return 'complete';
  // Else
  return 'partial';
};

export const computeWorkStatus = async id => {
  const query = {
    aggs: {
      work: {
        filter: {
          bool: {
            must: [{ term: { work_id: id } }, { term: { entity_type: 'Job' } }]
          }
        },
        aggs: {
          status: {
            terms: { field: 'job_status' }
          }
        }
      }
    }
  };
  const result = await el.search({ index: WORK_INDEX, body: query });
  const aggStatus = result.body.aggregations.work.status;
  const totalCount = pipe(
    map(e => e.doc_count),
    reduce(add, 0)
  )(aggStatus.buckets);
  const jobsDoneCount = pipe(
    filter(e => e.key === 'complete' || e.key === 'error'),
    map(e => e.doc_count),
    reduce(add, 0)
  )(aggStatus.buckets);
  const perStatus = map(
    e => ({ status: e.key, count: e.doc_count }),
    aggStatus.buckets
  );
  return {
    state: computeState(totalCount, perStatus),
    jobsCount: totalCount,
    jobsDoneCount,
    jobsPerState: perStatus
  };
};

export const workForEntity = async (entityId, args) => {
  return paginate(WORK_INDEX, {
    type: 'Work',
    connectionFormat: false,
    first: args.first,
    filters: {
      work_entity: entityId
    }
  });
};

export const workForConnector = async connectorId => {
  return paginate(WORK_INDEX, {
    type: 'Work',
    connectionFormat: false,
    first: 5,
    orderBy: 'created_at',
    orderMode: 'desc',
    filters: {
      connector_id: connectorId
    }
  });
};

export const loadFileWorks = async fileId => {
  return paginate(WORK_INDEX, {
    type: 'Work',
    connectionFormat: false,
    filters: {
      work_file: fileId
    }
  });
};

export const loadExportWorksAsProgressFiles = async entityId => {
  const works = await workForEntity(entityId, { first: 200 });
  // Filter if all jobs completed
  const worksWithStatus = await Promise.all(
    map(w => {
      return computeWorkStatus(w.work_id).then(status =>
        assoc('status', status, w)
      );
    }, works)
  );
  const onlyProgressWorks = filter(
    w => w.status === 'progress',
    worksWithStatus
  );
  return map(item => workToExportFile(item), onlyProgressWorks);
};

export const deleteWork = async workId => {
  return elDeleteByField(WORK_INDEX, 'work_id', workId);
};

export const deleteWorkForFile = async fileId => {
  const works = await loadFileWorks(fileId);
  await Promise.all(map(w => deleteWork(w.internal_id_key), works));
  return true;
};

export const initiateJob = workId => {
  const jobInternalId = uuid();
  return index(WORK_INDEX, {
    id: jobInternalId,
    internal_id_key: jobInternalId,
    grakn_id: jobInternalId,
    messages: ['Initiate work'],
    work_id: workId,
    created_at: now(),
    updated_at: now(),
    job_status: 'wait',
    entity_type: 'Job'
  });
};

export const createWork = async (connector, entityId = null, fileId = null) => {
  const workInternalId = uuid();
  return index(WORK_INDEX, {
    id: workInternalId,
    internal_id_key: workInternalId,
    grakn_id: workInternalId,
    work_id: workInternalId,
    entity_type: 'Work',
    connector_id: connector.id,
    work_entity: entityId,
    work_file: fileId,
    work_type: connector.connector_type,
    created_at: now(),
    updated_at: now()
  });
};

export const updateJob = async (jobId, status, messages) => {
  const job = await getAttributes(WORK_INDEX, jobId);
  const updatedJob = pipe(
    assoc('job_status', status),
    assoc('messages', messages),
    assoc('updated_at', now())
  )(job);
  await index(WORK_INDEX, updatedJob);
  return updatedJob;
};
