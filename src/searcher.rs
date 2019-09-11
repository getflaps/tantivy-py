#![allow(clippy::new_ret_no_self)]

use crate::document::Document;
use crate::query::Query;
use crate::to_pyerr;
use pyo3::prelude::*;
use pyo3::{exceptions, PyObjectProtocol};
use pyo3::types::{PyDict, PyTuple, PyList};
use tantivy as tv;
use std::collections::BTreeMap;


/// Tantivy's Searcher class
///
/// A Searcher is used to search the index given a prepared Query.
#[pyclass]
pub(crate) struct Searcher {
    pub(crate) inner: tv::LeasedItem<tv::Searcher>,
    pub(crate) schema: tv::schema::Schema,
}

#[pymethods]
impl Searcher {
    /// Search the index with the given query and collect results.
    ///
    /// Args:
    ///     query (Query): The query that will be used for the search.
    ///     collector (Collector): A collector that determines how the search
    ///         results will be collected. Only the TopDocs collector is
    ///         supported for now.
    ///
    /// Returns a list of tuples that contains the scores and DocAddress of the
    /// search results.
    ///
    /// Raises a ValueError if there was an error with the search.
    #[args(size = 10)]
    fn search(
        &self,
        py: Python,
        query: &Query,
        nhits: usize,
        facets: Option<&PyDict>
    ) -> PyResult<PyObject> {

        let top_collector = tv::collector::TopDocs::with_limit(nhits);

        let mut facets_collector = tv::collector::MultiCollector::new();

        let mut facets_requests = BTreeMap::new();

        // We create facets collector for each field and terms defined on the facets args
        if let Some(facets_dict) = facets {

            for key_value_any in facets_dict.items() {
                if let Ok(key_value) = key_value_any.downcast_ref::<PyTuple>() {
                    if key_value.len() != 2 {
                        continue;
                    }
                    let key: String = key_value.get_item(0).extract()?;
                    let field = self.schema.get_field(&key).ok_or_else(|| {
                        exceptions::ValueError::py_err(format!(
                            "Field `{}` is not defined in the schema.",
                            key
                        ))
                    })?;

                    let mut facet_collector = tv::collector::FacetCollector::for_field(field);

                    if let Ok(value_list) = key_value.get_item(1).downcast_ref::<PyList>() {
                        for value_element in value_list {
                            if let Ok(s) = value_element.extract::<String>() {
                                facet_collector.add_facet(&s);
                            }
                            
                        }
                        let facet_handler = facets_collector.add_collector(facet_collector);
                        facets_requests.insert(key, facet_handler);
                    }
                }
            }
        }

        let ret = self.inner.search(&query.inner, &(tv::collector::Count, top_collector, facets_collector));

        match ret {
            Ok((count, top, mut facets_tv_results)) => {
                let result = PyDict::new(py);

                result.set_item("count", count)?;

                let items: Vec<(f32, (u32, u32))> =
                    top.iter().map(|(f, d)| (*f, (d.segment_ord(), d.doc()))).collect();

                result.set_item("items", items)?;

                let mut facets_result: BTreeMap<String, Vec<(String, u64)>> =
                    BTreeMap::new();

                // Go though all collectors that are registered
                for (key, facet_collector) in facets_requests {
                    let facet_count = facet_collector.extract(&mut facets_tv_results);
                    let mut facet_vec = Vec::new();
                    if let Some(facets_dict) = facets {
                        match facets_dict.get_item(key.clone()) {
                            Some(facets_list_by_key) => {
                                if let Ok(facets_list_by_key_native) = facets_list_by_key.downcast_ref::<PyList>() {
                                    for facet_value in facets_list_by_key_native {
                                        if let Ok(s) = facet_value.extract::<String>() {
                                            let facet_value_vec: Vec<(&tv::schema::Facet, u64)> = facet_count
                                                .get(&s)
                                                .collect();

                                            // Go for all elements on facet and count to add on vector
                                            for (facet_value_vec_element, facet_count) in facet_value_vec {
                                                facet_vec.push((facet_value_vec_element.to_string(), facet_count))
                                            }
                                        }
                                    }
                                }
                            }
                            None => println!("Not found.")
                        }
                    }
                    facets_result.insert(key.clone(), facet_vec);
                }

                result.set_item("facets", facets_result)?;

                Ok(result.into())

            },
            Err(e) => Err(exceptions::ValueError::py_err(e.to_string())),
        }

    }

    /// Returns the overall number of documents in the index.
    #[getter]
    fn num_docs(&self) -> u64 {
        self.inner.num_docs()
    }

    /// Fetches a document from Tantivy's store given a DocAddress.
    ///
    /// Args:
    ///     doc_address (DocAddress): The DocAddress that is associated with
    ///         the document that we wish to fetch.
    ///
    /// Returns the Document, raises ValueError if the document can't be found.
    fn doc(&self, doc_address: &DocAddress) -> PyResult<Document> {
        let doc = self.inner.doc(doc_address.into()).map_err(to_pyerr)?;
        let named_doc = self.inner.schema().to_named_doc(&doc);
        Ok(Document {
            field_values: named_doc.0,
        })
    }

    fn docn(&self, seg_doc: &PyTuple) -> PyResult<Document> {
        let seg : u32 = seg_doc.get_item(0).extract()?;
        let doc : u32 = seg_doc.get_item(1).extract()?;
        let address = tv::DocAddress(seg, doc);
        let doc = self.inner.doc(address).map_err(to_pyerr)?;
        let named_doc = self.inner.schema().to_named_doc(&doc);
        Ok(Document {
            field_values: named_doc.0,
        })
    }

}


/// DocAddress contains all the necessary information to identify a document
/// given a Searcher object.
///
/// It consists in an id identifying its segment, and its segment-local DocId.
/// The id used for the segment is actually an ordinal in the list of segment
/// hold by a Searcher.
#[pyclass]
pub(crate) struct DocAddress {
    pub(crate) segment_ord: tv::SegmentLocalId,
    pub(crate) doc: tv::DocId,
}

#[pymethods]
impl DocAddress {
    /// The segment ordinal is an id identifying the segment hosting the
    /// document. It is only meaningful, in the context of a searcher.
    #[getter]
    fn segment_ord(&self) -> u32 {
        self.segment_ord
    }

    /// The segment local DocId
    #[getter]
    fn doc(&self) -> u32 {
        self.doc
    }
}

impl From<&tv::DocAddress> for DocAddress {
    fn from(doc_address: &tv::DocAddress) -> Self {
        DocAddress {
            segment_ord: doc_address.segment_ord(),
            doc: doc_address.doc(),
        }
    }
}

impl Into<tv::DocAddress> for &DocAddress {
    fn into(self) -> tv::DocAddress {
        tv::DocAddress(self.segment_ord(), self.doc())
    }
}

#[pyproto]
impl PyObjectProtocol for Searcher {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "Searcher(num_docs={}, num_segments={})",
            self.inner.num_docs(),
            self.inner.segment_readers().len()
        ))
    }
}
