resources_annotated_with_term <- function(search_term, include_subclasses=TRUE, direct_subclasses_only=FALSE) {
  ontology_name = "efo"
  con = openGWASSearch_dbconn()
  if ( include_subclasses )
    if ( direct_subclasses_only )
      ontology_table = paste0(ontology_name, "_edges")
    else
      ontology_table = paste0(ontology_name, "_entailed_edges")
  else
      ontology_table = paste0(ontology_name, "_edges")

  if( include_subclasses && !direct_subclasses_only )
    query = paste0("SELECT DISTINCT m.`SourceTermID` AS \"OpenGWASID\",",
                    "m.`SourceTerm` AS \"OpenGWAS Trait\", ",
                    "m.`MappedTermLabel` AS \"OntologyTerm\", m.`MappedTermCURIE` AS \"OntologyTermID\",",
                   "m.`MappingScore` AS \"MappingConfidence\" FROM `opengwas_trait_mappings` m LEFT JOIN ",
                  ontology_table, " ee ON (m.`MappedTermCURIE` = ee.subject)",
                 " WHERE (m.`MappedTermCURIE` = \"", search_term, "\" OR ee.object = \"", search_term, "\")")
  else
    query = paste0("SELECT DISTINCT m.`SourceTermID` AS \"OpenGWASID\",",
                   "m.`SourceTerm` AS \"OpenGWASTrait\", ",
                   "m.`MappedTermLabel` AS \"OntologyTerm\", m.`MappedTermCURIE` AS \"OntologyTermID\",",
                   "m.`MappingScore` AS \"MappingConfidence\" FROM `opengwas_trait_mappings` m LEFT JOIN ",
                   ontology_table, " ee ON (m.`MappedTermCURIE` = ee.subject)",
                   " WHERE (m.`MappedTermCURIE` =  \"", search_term, "\")")

  results = dbGetQuery(con, query)
  results$MappingConfidence = round(results$MappingConfidence, digits=3)
  return(results)
}

##internal helper function used to build the two data resources - this
##function needs to be run and the data updated whenever a new database is obtained
.makeCorpus = function(path=getwd(), use_stemming=TRUE, remove_stop_words=TRUE, save=TRUE){
  efo_df = dbGetQuery(openGWASSearch_dbconn(),"SELECT * from efo_labels")
  efo_tc = create_tcorpus(efo_df, doc_column = 'Subject', text_columns = 'Object')
  efo_tc$preprocess(use_stemming = use_stemming, remove_stopwords=remove_stop_words)

  if( save ) {
   save(efo_tc, file= paste0(path, "/efo_tc.rda"), compress="xz")
   save(efo_df, file=paste0(path, "/efo_df.rda"), compress="xz")
  }
  return(efo_tc)
}

hits2DT = function(hits, efoDF, tc) {
  if(nrow(hits$hits)==0) return(NULL)
  ##fix up the tc so it only has the HITS...
  .i = tc$get_token_id(doc_id = hits$hits$doc_id, token_id = hits$hits$token_id)
  .value = as.character(hits$hits$code)
  tc$set("HITS", value = .value, subset = .i, subset_value = FALSE)
  keep=rep(FALSE, nrow(tc$tokens))
  keep[.i] = TRUE
  sub_tc = tc$subset(subset = keep, subset_meta = unique(hits$hits$doc_id %in% tc$meta$doc_id),
                     copy=TRUE)

  hit_index = match(unique(hits$hits$doc_id), efoDF$Subject)
  matchedEFO = efoDF[hit_index,]
  EFO = paste0("<a href= \"", matchedEFO$IRI, "\">", matchedEFO$Subject, "</a>")
  EFOtext = matchedEFO$Object
  Direct = matchedEFO$"Direct"
  Inherited = matchedEFO$"Inherited"
  outDF = data.frame(EFO=EFO, Text=EFOtext, Direct=Direct, Inherited=Inherited, row.names=matchedEFO$Subject)
  ##now look at how many things were queried and make a vector for each one
  ##that gives the token for the doc
  Queries = split(sub_tc$tokens, sub_tc$tokens$HITS)
  for( i in 1:length(Queries)) {
    tmp = rep(NA, nrow(matchedEFO))
    names(tmp) = matchedEFO$Subject
    docID = as.character(Queries[[i]]$doc_id)
    tmp[docID] = as.character(Queries[[i]]$token)
    outDF[[names(Queries[i])]] = tmp
  }
    return(outDF)
}


