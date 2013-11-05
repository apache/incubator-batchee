/*
 * Copyright 2012 International Business Machines Corp.
 * 
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.apache.batchee.container.jsl;

import java.util.ArrayList;
import java.util.List;

public class GlobPatternMatcherImpl {
    public boolean matchWithoutBackslashEscape(final String toMatch, final String pattern) {
        // Precondition: Blow up with null or empty s or pattern.
        //TODO - Spec ramifications or is this already covered by property and exit status defaulting?
        if ((pattern == null) || (pattern.length() == 0) ||
            (toMatch == null) || (toMatch.length() == 0)) {
            throw new IllegalArgumentException("Pattern = " + pattern +
                "and string to match = " + toMatch +
                ", but both pattern and to-match String are required to be non-null Strings with length >=1 ");
        }

        // Normalize away consecutive '*' chars:
        String newPattern = pattern;
        while (true) {
            if (newPattern.contains("**")) {
                newPattern = newPattern.replace("**", "*");
            } else {
                break;
            }
        }

        return recursiveMatch(toMatch, newPattern);
    }

    //
    // TODO - confirm in spec that there is NO backslashing of '*', '?'.
    //
    private boolean recursiveMatch(final String toMatch, final String subpattern) {
        final int firstAsterisk = subpattern.indexOf("*");
        final int secondAsterisk = subpattern.indexOf("*", firstAsterisk + 1);
        final int lastAsterisk = subpattern.lastIndexOf("*");

        //
        //  A) If there are no '*'(s), do a non-wildcard (except for ?) match
        //
        if (firstAsterisk == -1) {
            return matchNoAsterisk(toMatch, subpattern);
        }

        //
        //  B) Match off any beginning or end
        //

        // Match any beginning BEFORE the first asterisk
        if (firstAsterisk > 0) {
            final String beginPattern = subpattern.substring(0, firstAsterisk);
            if (toMatch.length() < beginPattern.length()) {
                return false;
            }
            final String beginToMatch = toMatch.substring(0, firstAsterisk);
            if (!matchNoAsterisk(beginToMatch, beginPattern)) {
                return false;
            }
        }

        // This will just be a straight copy if 'firstAsterisk' = 0.
        // Since we're using recursion, try to be a bit performance-sensitive and not do this until necessary.
        String remainingToMatch = toMatch.substring(firstAsterisk);
        //Match any end AFTER the first asterisk
        if (lastAsterisk < subpattern.length() - 1) {
            final String endPattern = subpattern.substring(lastAsterisk + 1);
            if (remainingToMatch.length() < endPattern.length()) {
                return false;
            }
            // Give me a substring the size of 'endPattern' at the end
            final String endToMatch = remainingToMatch.substring(remainingToMatch.length() - endPattern.length(), remainingToMatch.length());
            if (!matchNoAsterisk(endToMatch, endPattern)) {
                return false;
            }
            // Already matched against char at index: 'remainingToMatch.length() - endPattern.length()', so truncate immediately before.
            remainingToMatch = remainingToMatch.substring(0, remainingToMatch.length() - endPattern.length());
        }

        // C) Now I either have:
        //  1)   *
        //  2)   *-xxxxx-*  (All non-'*' chars in the middle
        //  3)   *-xy-*-x-* (At least one '*' in the middle)
        if (firstAsterisk == lastAsterisk) {
            return true;
        } else if (secondAsterisk == lastAsterisk) {
            final String middlePattern = subpattern.substring(firstAsterisk + 1, lastAsterisk);
            if (remainingToMatch.length() < middlePattern.length()) {
                return false;
            } else {
                for (int i = 0; i <= remainingToMatch.length() - middlePattern.length(); i++) {
                    final String matchCandidate = remainingToMatch.substring(i, i + middlePattern.length());
                    if (matchNoAsterisk(matchCandidate, middlePattern)) {
                        return true;
                    }
                }
                return false;
            }
        } else {
            //
            // Now I have to match against sub-sub-pattern *xxx*xy*.  The strategy here is to use recursion.

            // 1. Isolate 'xxx' and store as 'patternBetweenAsterisk1and2'
            final String patternBetweenAsterisk1and2 = subpattern.substring(firstAsterisk + 1, secondAsterisk);

            // 2. Find every place in the string matching 'xxx' and store the remainder as a candidate
            final List<String> subMatchCandidates = new ArrayList<String>();
            for (int i = 0; i <= remainingToMatch.length() - patternBetweenAsterisk1and2.length(); i++) {
                final String matchCandidate = remainingToMatch.substring(i, i + patternBetweenAsterisk1and2.length());
                if (matchNoAsterisk(matchCandidate, patternBetweenAsterisk1and2)) {
                    // Grab the part of the string after the match.
                    String subMatchCandidate = remainingToMatch.substring(i + patternBetweenAsterisk1and2.length());
                    subMatchCandidates.add(subMatchCandidate);
                }
            }

            if (subMatchCandidates.size() == 0) {
                return false;
            }

            // 2. Calculate the new pattern to match against.
            // For "*xxx*xy*" this would be "*xy*".
            // For "*xxx*xy*z*" this would be "*xy*z*"
            final String nestedPattern = subpattern.substring(secondAsterisk, lastAsterisk + 1);  // We want to include the last asterisk.

            // 3. try matching each one
            for (final String candidate : subMatchCandidates) {
                if (recursiveMatch(candidate, nestedPattern)) {
                    return true;
                }
            }
            return false;
        }
    }


    /**
     * 1. Match (against regular char or ?)
     * Default to 'true' which would apply to zero-length boundary condition
     */
    private boolean matchNoAsterisk(final String toMatch, final String subpattern) {
        final int toMatchLen = toMatch.length();
        final int subpatternLen = subpattern.length();

        if (toMatchLen != subpatternLen) {
            return false;
        }

        for (int i = 0; i < toMatchLen; i++) {
            if (subpattern.substring(i, i + 1).equals("*")) {
                throw new IllegalStateException("Shouldn't have encountered a '*' in matchNoAsterisk, toMatch = " + toMatch + ", subPattern = " + subpattern);
            } else if (!subpattern.substring(i, i + 1).equals("?")) {
                if (subpattern.charAt(i) != toMatch.charAt(i)) {
                    return false;
                }
                // else continue
            }
        }

        return true;
    }
}
