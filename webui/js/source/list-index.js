/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

let LOTS_OF_LISTS = 25; // Beyond 25 list domains and we start using the old phonebook.
let list_json = {}

function list_index(state, json) {
    if (json) {
        list_json = json;
    }
    let letter = 'a';
    let lists = document.getElementById('list_picker_ul');
    if (state && state.letter) {
        letter = state.letter;
        for (let xtab of lists.childNodes) {
            if (xtab.innerText == state.letter) {
                xtab.setAttribute("class", 'active');
            } else if (xtab.setAttribute) {
                xtab.setAttribute("class", "");
            }
        }
    } else {
        let letters = 'abcdefghijklmnopqrstuvwxyz';
        for (var i = 0; i < letters.length; i++) {
            let letter = letters[i].toUpperCase();
            let li = new HTML('li', {
                onclick: 'list_index({letter: "%s"});'.format(letter),
                class: (letter == 'A') ? 'active' : null
            }, letter);
            lists.inject(li);
        }
    }

    let list_ul = document.getElementById('list_index_wide_lists');
    list_ul.textContent = "";
    for (let domain_name in list_json.lists) {
        if (domain_name.toLowerCase().startsWith(letter.toLowerCase())) {
            console.log(domain_name);
            let li = new HTML('li', {});
            let a = new HTML('a', {
                href: 'list.html?%s'.format(domain_name)
            }, domain_name);
            li.inject(a);
            list_ul.inject(li);
        }
    }
}


let preferred_lists = ['dev', 'users'];
let preferred_no_lists = ['security'];

function best_list(entries) {
    let x = 0;
    let pick = 'dev';
    for (var key in entries) {
        if (preferred_lists.has(key)) return key;
        if (preferred_no_lists.has(key) && Object.keys(entries).length > 1) continue;
        if (entries[key] > x) {
            x = entries[key];
            pick = key;
        }
    }
    return pick;
}

function list_index_onepage(state, json) {
    let obj = document.getElementById('list_index_child');
    obj.style.padding = '8px';
    let domains = Object.keys(json.lists);
    domains.sort();
    let letter = '';
    for (var i = 0; i < domains.length; i++) {
        let domain = domains[i];
        let l = domain[0];
        if (l != letter) {
            letter = l;
            let lhtml = new HTML('h2', {}, l.toUpperCase());
            obj.inject(lhtml);
        }
        let a = new HTML('a', {
            href: 'list.html?%s@%s'.format(best_list(json.lists[domain]), domain)
        }, domain);
        obj.inject(['- ', a]);
        obj.inject(new HTML('br'));
    }
    if (domains.length > LOTS_OF_LISTS) {
        list_index(state, json);
    } else {
        let wide_obj = document.getElementById('list_index_child_wide');
        let new_obj = obj.cloneNode(true);
        new_obj.setAttribute("id", "list_index_child_wide");
        wide_obj.replaceWith(new_obj);
        console.log(new_obj);
    }
}

function prime_list_index() {
    GET('%sapi/preferences.lua'.format(apiURL), list_index_onepage, {});
}