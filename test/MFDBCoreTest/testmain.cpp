//
// Copyright (c) 2014-2015 Benedetto Proietti
//
//
//  This program is free software: you can redistribute it and/or  modify
//  it under the terms of the GNU Affero General Public License, version 3,
//  as published by the Free Software Foundation.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.
//
//  As a special exception, the copyright holders give permission to link the
//  code of portions of this program with the OpenSSL library under certain
//  conditions as described in each individual source file and distribute
//  linked combinations including the program with the OpenSSL library. You
//  must comply with the GNU Affero General Public License in all respects for
//  all of the code used other than as permitted herein. If you modify file(s)
//  with this exception, you may extend this exception to your version of the
//  file(s), but you are not obligated to do so. If you do not wish to do so,
//  delete this exception statement from your version. If you delete this
//  exception statement from all source files in the program, then also delete
//  it in the license file.



void Test_Bin();
void Test_FilterValue(bool = false);
void Test_FilterQuery1();
void test_QE_Collections_insert_simple1();
void Prepare_QA();
void test_query_1_adhoc();
void Test_QP_AND2();
void Test_Serialization();
void Test_Aggregate1();

int main()
{
    Prepare_QA();

    Test_Aggregate1();

    Test_FilterValue(true);
    Test_FilterValue(false);

    test_QE_Collections_insert_simple1();
    Test_Bin();
    Test_QP_AND2();

    return 0;
}

