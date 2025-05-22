The files included for the Russian Expert to look at are:
* Russian_sample_word_forms.csv
  * Column: entry_id
    * An integer linking it back to the dictionary form
  * Column: word
    * The dictionary form
  * Column: lang
    * The language the word entry is for
  * Column: pos
    * Part of speech
  * Column: form
    * The word form. For Russian, this is mostly inflections.
  * Columns: head_nr, roman, source
    * Some dictionary form header information, romanization, and the method of generating the word form (inflection, conjugation, etc.)
  * Column: tags
    * The tags associated with the word form. These are separated by a "/"
  * Column: "lang/pos/form_tag"
    * The group it was sampled from. 
* {lang}_form_tags_by_pos.csv
  * Column: form_tags
    * Tags that show up in the {lang} dataset
  * Column: {lang}
    * The parts of speech where the tag has shown up
* word_form_properties.json
  * A dictionary (keyword --> value) where the keys describe a property of words that I think show up in multiple languages.
  * Keys:  number, case, gender, tense, mood, aspect, etc.
  * Values for each of these: tags that describe the possible values of each property. Tags are appropriate for multiple languages. 
* {lang}_pos_properties.json
  * A dictionary describing which properties of words matter for different parts of speech in Russian.
  * Keys: verb, noun, det(erminer), pron(oun), adj(ective), etc.
  * Values for each of these: properties as described in word_form_properties.json
