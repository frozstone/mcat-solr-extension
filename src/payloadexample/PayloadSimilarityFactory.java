package payloadexample;

import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.schema.SimilarityFactory;

public class PayloadSimilarityFactory extends SimilarityFactory {
  @Override
  public void init(SolrParams params) {
    super.init(params);
  }

  @Override
  public Similarity getSimilarity() {
    return new PayloadSimilarity();
  }
}

class PayloadSimilarity extends DefaultSimilarity {

  //Here's where we actually decode the payload and return it.
  @Override
  public float scorePayload(int doc, int start, int end, BytesRef payload) {
    if (payload == null) return 1.0F;
    //return (float)Math.log(PayloadHelper.decodeFloat(payload.bytes, payload.offset));  
    return PayloadHelper.decodeFloat(payload.bytes, payload.offset);
  }
}