#region License, Terms and Author(s)
//
// NotWebMatrix
// Copyright (c) 2012 Atif Aziz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#endregion

namespace NotWebMatrix.Data
{
    #region Imports

    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.ComponentModel;
    using System.Data;
    using System.Dynamic;
    using System.Linq;

    #endregion

    /// <summary>
    /// Represents a data record by using a custom type descriptor and the 
    /// capabilities of the Dynamic Language Runtime (DLR).
    /// </summary>

    [Obsolete]
    public sealed class DynamicRecord : DynamicObject, ICustomTypeDescriptor
    {
        readonly IDataRecord _record;
        ReadOnlyCollection<string> _columns;
        
        public DynamicRecord(IDataRecord record)
        {
            if (record == null) throw new ArgumentNullException("record");
            _record = record;
        }

        public object this[string name] { get { return GetValue(_record[name]); } }
        public object this[int index] { get { return GetValue(_record[index]); } }
        static object GetValue(object value) { return !Convert.IsDBNull(value) ? value : null; }

        public IList<string> Columns 
        { 
            get 
            {
                if (_columns == null)
                {
                    var record = _record;
                    var names = from i in Enumerable.Range(0, record.FieldCount)
                                select record.GetName(i);
                    _columns = Array.AsReadOnly(names.ToArray());
                }
                return _columns;
            } 
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = this[binder.Name];
            return true;
        }

        public override IEnumerable<string> GetDynamicMemberNames() { return Columns; }

        #region ICustomTypeDescriptor

        AttributeCollection ICustomTypeDescriptor.GetAttributes() { return AttributeCollection.Empty; }
        string ICustomTypeDescriptor.GetClassName() { return null; }
        string ICustomTypeDescriptor.GetComponentName() { return null; }
        TypeConverter ICustomTypeDescriptor.GetConverter() { return null; }
        EventDescriptor ICustomTypeDescriptor.GetDefaultEvent() { return null; }
        PropertyDescriptor ICustomTypeDescriptor.GetDefaultProperty() { return null; }
        object ICustomTypeDescriptor.GetEditor(Type editorBaseType) { return null; }
        EventDescriptorCollection ICustomTypeDescriptor.GetEvents(Attribute[] attributes) { return EventDescriptorCollection.Empty; }
        EventDescriptorCollection ICustomTypeDescriptor.GetEvents() { return EventDescriptorCollection.Empty; }
        PropertyDescriptorCollection ICustomTypeDescriptor.GetProperties(Attribute[] attributes) { return ((ICustomTypeDescriptor)this).GetProperties(); }
        object ICustomTypeDescriptor.GetPropertyOwner(PropertyDescriptor pd) { return this; }

        PropertyDescriptorCollection ICustomTypeDescriptor.GetProperties()
        {
            var record = _record;
            var columns = Columns;
            var properties =
                from i in Enumerable.Range(0, columns.Count)
                select (PropertyDescriptor) new DynamicPropertyDescriptor(columns[i], record.GetFieldType(i));
            return new PropertyDescriptorCollection(properties.ToArray(), readOnly: true);
        }

        #endregion

        class DynamicPropertyDescriptor : PropertyDescriptor
        {
            static readonly Attribute[] _empty = new Attribute[0];
            readonly Type _type;

            public DynamicPropertyDescriptor(string name, Type type) :
                base(name, _empty)
            {
                _type = type;
            }

            public override object GetValue(object component)
            {
                var record = component as DynamicRecord;
                return record != null ? record[Name] : null;
            }

            public override Type ComponentType { get { return typeof(DynamicRecord); } }
            public override bool IsReadOnly { get { return true; } }
            public override Type PropertyType { get { return _type; } }
            public override bool CanResetValue(object component) { return false; }
            public override void ResetValue(object component) { throw CreateRecordReadOnlyException(); }
            public override void SetValue(object component, object value) { throw CreateRecordReadOnlyException(); }
            public override bool ShouldSerializeValue(object component) { return false; }

            Exception CreateRecordReadOnlyException()
            {
                var message = string.Format(@"Unable to modify the value of column ""{0}"" because the record is read only.", Name);
                return new InvalidOperationException(message);
            }
        }
    }
}
