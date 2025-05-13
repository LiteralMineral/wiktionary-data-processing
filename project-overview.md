
Welcome to my project. In this project, I'll be using data from https://kaikki.org/ to generate flashcard data, importable to a program like Anki (https://apps.ankiweb.net/).
The end product will have two central modes:
* data collection/reformatting
* vocab query and export






The current status is as follows:

* data collection/reformatting
  * Download the target dataset .................... [satisfactory]
  * Record basic information for each entry (information that is not nested) .............................[satisfactory]
  * Process inflection data: [in progress]
    * Analyze and save all the tags associated with each datasets' parts of speech. ........................ [satisfactory]
    * Prompt user to identify categories of inflectional tags and sort them. Save in a file for later use [not yet started]
    * Using file identifying inflectional tags for each part of speech, identify inflectional forms for each word entry. [in progress] <br/> (I have code from a previous project that successfully did this. I need to adapt it for this project.)
  * Process dictionary data: [not yet started]
    * ...
    * ...
  * ...
  * More procedures to be planned.

To set up your own fork of this...:
* Set up a server for yourself
* Copy the config.ini.template file and fill in the necessary information to connect to your server.
* 

The different stages of the data processing:
* download and assign unique ids for each dataset
* analyze the tags associated with each part of speech
* inflectional processing:
  * edit the programmed associations
  * using those programmed associations, create combo-tags and pull the inflections out.
* Storing the results of the different data processing stages in their own files to be transferred to the sql database...
* 

When you initialize a dataset in the sql server:
* download using spark
* parse each nested column's schema as a json string. store it in json_schemas
* convert each nested column to a json string. store it in json_info <br/>(insert the item, do not overwrite!)
* ...

local data structure
* other files/directories in project
* Data/
  * {lang}
    * {lang}_kaikki_data.jsonl
    * {lang}_download_metadata  <br/> metadata about when the data was downloaded, from where, etc.....
    * has_id_column/  <br/> (this is a parquet file. or. more likely, several, in a directory.)
    * {lang}_tags/  <br/> (this is a parquet file with a dataframe of a language's tags sorted by the column they come from and the part of speech they are relevant to.)

wiktionary_app database tables
* Tables (autoincrement id for json_info!!)
  * basic_info (index by language, word, and pos)
    * entry_id
    * word
    * pos
    * lang
    * lang_code
    * source
    * original_title
    * etymology_number
    * etymology_text
  * word_forms (indexed by language, word, and part of speech)
    * word
    * language
    * part_of_speech
    * form
    * (assorted columns with tags that communicate different information)
  * tag_set (index by language and part of speech)
    * language (text)
    * part_of_speech (text)
    * tag_category (text) This describes the trait of a word the tags are trying to capture.
    * tags_to_capture (array)


[//]: # "* json_info (index by language, word, and pos)
    * entry_id
    * word
    * pos
    * lang
    * lang_code
    * source
    * original_title
    * etymology_number
    * etymology_text
    * abbreviations
    * antonyms
    * categories
    * coordinate_terms
    * derived
    * descendants
    * etymology_templates
    * form_of
    * forms
    * head_templates
    * holonyms
    * hypernyms
    * hyphenation
    * hyponyms
    * inflection_templates
    * info_templates
    * meronyms
    * proverbs
    * related
    * senses
    * sounds
    * source
    * synonyms
    * wikipedia
  * json_schema (index by language and column_name)
    * language
    * column_name
  * json_schema"



Problem: How to tell what tags should be extracted for each column?
Planned Solution: 
Get feedback from language experts....

* Set of all Languages $L = \{ english, korean, russian, japanese, \dots, spanish, french, german, chinese, arabic\}$
* Set of all Words $w$ with $w \in l \wedge l |in L$
* Set of Tags $T = \{masculine, feminine, neuter, plural, dual, animate, \dots, inanimate, past,  Moscow, Portugal, Hangul, Hiragana\}$
* Set of Tag Categories $C_t = \{number, person, tense, aspect, \dots,  animacy, gender, mood, writing\_system, usage\}$
* Set of Dataframe Columns $D = \{forms, lang, lang\_code, word, \dots, pos, senses, head\_templates\}$
* Set of Parts of Speech $P = \{ noun, verb, pronoun, character, prep, prep\_phrase, num, postp, intj, \dots,  name, prefix, adnominal, soft-redirect\}$
* Function $get\_word\_tags: W \rightarrow powerset(T)$
  * given:
    * word $w \in W$
  * returns:
    * tagset $t \in T$
* Function $get\_category\_tags: C \rightarrow powerset(T)$
  * given:
    * category $C$
  * returns:
    * tagset $t \in powerset(T)$
* Function $get\_lang\_pos\_categories: L \times P\rightarrow powerset(C)$
  * given:
    * language $l \in L$
    * part of speech $p \in P$
  * return
    * category_set $c \in powerset(C)$
* Function $get\_word\_combined\_tags: W, L, P, F|(F: L \times P \rightarrow powerset(C)) \rightarrow powerset(T)$
  * given
    * language $l \in L$
    * part of speech $p \in P$
    * word $w \in W \wedge w \in l \wedge w \in p$
    * 
