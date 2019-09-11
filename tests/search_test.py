import tantivy
import pytest

from tantivy import Document, Index, SchemaBuilder, Schema, Facet


def schema():
    return (
        SchemaBuilder()
        .add_text_field("title", stored=True)
        .add_text_field("body")
        .add_facet_field("facets")
        .build()
    )


@pytest.fixture(scope="class")
def ram_index():
    # assume all tests will use the same documents for now
    # other methods may set up function-local indexes
    index = Index(schema())
    writer = index.writer()

    # 2 ways of adding documents
    # 1
    doc = Document()
    # create a document instance
    # add field-value pairs
    doc.add_text("title", "The Old Man and the Sea")
    doc.add_text(
        "body",
        (
            "He was an old man who fished alone in a skiff in"
            "the Gulf Stream and he had gone eighty-four days "
            "now without taking a fish."
        ),
    )

    doc.add_facet("facets", Facet.from_string("/category/category1"))
    writer.add_document(doc)

    doc = Document()
    # create a document instance
    # add field-value pairs
    doc.add_text("title", "The Old Man and the Sea")
    doc.add_text(
        "body",
        (
            "He was an old man who fished alone in a skiff in"
            "the Gulf Stream and he had gone eighty-four days "
            "now without taking a fish."
        ),
    )

    doc.add_facet("facets", Facet.from_string("/category/category2"))
    writer.add_document(doc)

    # 2 use the built-in json support
    # keys need to coincide with field names
    doc = Document.from_dict(
        {
            "title": "Of Mice and Men",
            "body": (
                "A few miles south of Soledad, the Salinas River drops "
                "in close to the hillside bank and runs deep and "
                "green. The water is warm too, for it has slipped "
                "twinkling over the yellow sands in the sunlight "
                "before reaching the narrow pool. On one side of the "
                "river the golden foothill slopes curve up to the "
                "strong and rocky Gabilan Mountains, but on the valley "
                "side the water is lined with trees—willows fresh and "
                "green with every spring, carrying in their lower leaf "
                "junctures the debris of the winter’s flooding; and "
                "sycamores with mottled, white, recumbent limbs and "
                "branches that arch over the pool"
            ),
        }
    )
    writer.add_document(doc)
    writer.add_json(
        """{
            "title": ["Frankenstein", "The Modern Prometheus"],
            "body": "You will rejoice to hear that no disaster has accompanied the commencement of an enterprise which you have regarded with such evil forebodings.  I arrived here yesterday, and my first task is to assure my dear sister of my welfare and increasing confidence in the success of my undertaking."
        }"""
    )
    writer.commit()
    index.reload()
    return index


class TestClass(object):
    def test_simple_facets_search(self, ram_index):
        index = ram_index
        query = index.parse_query("sea whale", ["title", "body"])

        result = index.searcher().search(query, nhits=10, facets={"facets": ["/category"]})
        assert result["count"] == 2
        assert len(result["facets"]["facets"]) == 2
        _, doc_address = result["items"][0]
        searched_doc = index.searcher().docn(doc_address)
        assert searched_doc["title"] == ["The Old Man and the Sea"]

        query = index.parse_query("sea whale", ["title", "body"], filters={"facets": ["/category/category1"]})
        result = index.searcher().search(query, nhits=10, facets={"facets": ["/category"]})
        assert result["count"] == 1
        assert len(result["facets"]["facets"]) == 1
        _, doc_address = result["items"][0]
        searched_doc = index.searcher().docn(doc_address)
        assert searched_doc["title"] == ["The Old Man and the Sea"]

        query = index.parse_query("sea whale", ["title", "body"], filters={"facets": ["/category"]})
        result = index.searcher().search(query, nhits=10, facets={"facets": ["/category"]})
        assert result["count"] == 2
        assert len(result["facets"]["facets"]) == 2
        _, doc_address = result["items"][0]
        searched_doc = index.searcher().docn(doc_address)
        assert searched_doc["title"] == ["The Old Man and the Sea"]
