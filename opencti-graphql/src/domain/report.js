import { head } from 'ramda';
import uuid from 'uuid/v4';
import { delEditContext, setEditContext } from '../database/redis';
import {
  createRelation,
  deleteByID,
  editInputTx,
  loadByID,
  notify,
  now,
  paginate,
  qk,
  queryAll,
  prepareDate,
  takeTx
} from '../database/grakn';
import { BUS_TOPICS } from '../config/conf';

export const findAll = args => paginate('match $m isa Report', args);

export const findById = reportId => loadByID(reportId);

export const createdByRef = (reportId, args) =>
  queryAll(
    `match $x isa Identity; 
    $rel(creator:$x, so:$report) isa created_by_ref; 
    $report id ${reportId}; get $x;`,
    args
  ).then(r => head(r)); // Return the unique result;

export const markingDefinitions = (reportId, args) =>
  paginate(
    `match $marking isa Marking-Definition; 
    $rel(marking:$marking, so:$report) isa object_marking_refs; 
    $report id ${reportId}`,
    args
  );

export const objectRefs = (reportId, args) =>
  paginate(
    `match $so isa Stix-Domain; 
    $rel(so:$so, knowledge_aggregation:$report) isa object_refs; 
    $report id ${reportId}`,
    args
  );

export const addReport = async (user, report) => {
  const wTx = await takeTx();
  const reportIterator = await wTx.query(`insert $report isa Report 
    has type "report";
    $report has stix_id "report--${uuid()}";
    $report has name "${report.name}";
    $report has description "${report.description}";
    $report has published ${prepareDate(report.published)};
    $report has report_class "${report.report_class}";
    $report has created ${now()};
    $report has modified ${now()};
    $report has revoked false;
    $report has created_at ${now()};
    $report has updated_at ${now()};
  `);
  const createdReport = await reportIterator.next();
  const createdReportId = await createdReport.map().get('report').id;

  if (report.createdByRef) {
    await wTx.query(`match $from id ${createdReportId};
         $to id ${report.createdByRef};
         insert (so: $from, creator: $to)
         isa created_by_ref;`);
  }
  await wTx.commit();

  return loadByID(createdReportId).then(created =>
    notify(BUS_TOPICS.Report.ADDED_TOPIC, created, user)
  );
};

export const reportDelete = reportId => deleteByID(reportId);

export const reportDeleteRelation = relationId => deleteByID(relationId);

export const reportAddRelation = (user, reportId, input) =>
  createRelation(reportId, input).then(report =>
    notify(BUS_TOPICS.Report.EDIT_TOPIC, report, user)
  );

export const reportCleanContext = (user, reportId) => {
  delEditContext(user, reportId);
  return loadByID(reportId).then(report =>
    notify(BUS_TOPICS.Report.EDIT_TOPIC, report, user)
  );
};

export const reportEditContext = (user, reportId, input) => {
  setEditContext(user, reportId, input);
  loadByID(reportId).then(report =>
    notify(BUS_TOPICS.Report.EDIT_TOPIC, report, user)
  );
};

export const reportEditField = (user, reportId, input) =>
  editInputTx(reportId, input).then(report =>
    notify(BUS_TOPICS.Report.EDIT_TOPIC, report, user)
  );
