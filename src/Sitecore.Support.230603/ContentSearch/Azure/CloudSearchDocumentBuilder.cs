namespace Sitecore.Support.ContentSearch.Azure
{
  using System;
  using System.Collections.Concurrent;
  using System.Collections.Generic;
  using System.Linq;
  using System.Reflection;
  using Sitecore.Data;
  using Sitecore.ContentSearch;

  public class CloudSearchDocumentBuilder : Sitecore.ContentSearch.Azure.CloudSearchDocumentBuilder
  {
    public CloudSearchDocumentBuilder(IIndexable indexable, IProviderUpdateContext context) : base(indexable, context)
    {

    }

    private static readonly MethodInfo CheckAndAddFieldMethodInfo =
      typeof(Sitecore.ContentSearch.Azure.CloudSearchDocumentBuilder).BaseType.GetMethod("CheckAndAddField",
        BindingFlags.Instance | BindingFlags.NonPublic);

    public override void AddItemFields()
    {
      try
      {
        VerboseLogging.CrawlingLogDebug(() => "AddItemFields start");

        if (this.Options.IndexAllFields)
        {
          this.Indexable.LoadAllFields();
        }

        var loadedFields = new HashSet<string>(this.Indexable.Fields.Select(f => f.Id.ToString()));
        var includedFields = new HashSet<string>();
        if (this.Options.HasIncludedFields)
        {
          includedFields = new HashSet<string>(this.Options.IncludedFields);
        }

        includedFields.ExceptWith(loadedFields);

        if (IsParallel)
        {
          var exceptions = new ConcurrentQueue<Exception>();

          this.ParallelForeachProxy.ForEach(this.Indexable.Fields, this.ParallelOptions, f =>
          {
            try
            {
              CheckAndAddFieldMethodInfo.Invoke(this, new object[] {this.Indexable, f});
            }
            catch (Exception ex)
            {
              exceptions.Enqueue(ex);
            }
          });

          if (exceptions.Count > 0)
          {
            throw new AggregateException(exceptions);
          }

          if (!this.Options.IndexAllFields && this.Options.HasIncludedFields)
          {
            this.ParallelForeachProxy.ForEach(includedFields, this.ParallelOptions, fieldId =>
            {
              try
              {
                ID id;
                if (ID.TryParse(fieldId, out id))
                {
                  var field = this.Indexable.GetFieldById(id);
                  if (field != null)
                  {
                    CheckAndAddFieldMethodInfo.Invoke(this,
                      new object[] {this.Indexable, field});
                  }
                }
              }
              catch (Exception ex)
              {
                exceptions.Enqueue(ex);
              }
            });
          }
        }
        else
        {
          foreach (var field in this.Indexable.Fields)
          {
            CheckAndAddFieldMethodInfo.Invoke(this, new object[] {this.Indexable, field});
          }

          if (!this.Options.IndexAllFields && this.Options.HasIncludedFields)
          {
            foreach (var fieldId in includedFields)
            {
              ID id;
              if (ID.TryParse(fieldId, out id))
              {
                var field = this.Indexable.GetFieldById(id);
                if (field != null)
                {
                  CheckAndAddFieldMethodInfo.Invoke(this,
                    new object[] {this.Indexable, field});
                }
              }
            }
          }
        }
      }
      finally
      {
        VerboseLogging.CrawlingLogDebug(() => "AddItemFields End");
      }
    }
  }
}