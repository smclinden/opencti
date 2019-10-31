import { el, reindex } from '../database/elasticSearch';
import { WORK_INDEX } from '../domain/work';

module.exports.up = async next => {
  // Create the new index if not exist
  const existWorkIndex = await el.indices.exists({ index: WORK_INDEX });
  if (existWorkIndex.body === false) {
    await el.indices.create({
      index: WORK_INDEX,
      body: {
        mappings: {
          properties: {
            id: { type: 'keyword' },
            grakn_id: { type: 'keyword' },
            internal_id_key: { type: 'keyword' },
            work_id: { type: 'keyword' },
            work_type: { type: 'keyword' },
            work_entity: { type: 'keyword' },
            job_status: { type: 'keyword' },
            entity_type: { type: 'keyword' }
          }
        }
      }
    });
  }
  // If old index exist, create mappings and reindex to correct index
  const indexExist = await el.indices.exists({ index: 'work_jobs_index' });
  if (indexExist.body) {
    // If old index, patch some fields, we need to reindex
    await reindex([{ source: 'work_jobs_index', dest: WORK_INDEX }]);
    await el.indices.delete({ index: 'work_jobs_index' });
  }
  next();
};

module.exports.down = next => {
  next();
};
