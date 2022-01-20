import React from 'react';
import { useHistory, useParams } from 'react-router';
import { useForm, Controller, FormProvider } from 'react-hook-form';
import {
  CompatibilityLevelCompatibilityEnum,
  SchemaType,
} from 'generated-sources';
import { clusterSchemaPath } from 'lib/paths';
import { NewSchemaSubjectRaw } from 'redux/interfaces';
import Editor from 'components/common/Editor/Editor';
import Select from 'components/common/Select/Select';
import { Button } from 'components/common/Button/Button';
import { InputLabel } from 'components/common/Input/InputLabel.styled';
import PageHeading from 'components/common/PageHeading/PageHeading';
import { useAppDispatch, useAppSelector } from 'lib/hooks/redux';
import {
  schemasApiClient,
  selectSchemaById,
} from 'redux/reducers/schemas/schemasSlice';
import { serverErrorAlertAdded } from 'redux/reducers/alerts/alertsSlice';
import { getResponse } from 'lib/errorHandling';

import * as S from './Edit.styled';

const Edit: React.FC = () => {
  const history = useHistory();
  const dispatch = useAppDispatch();

  const { clusterName, subject } =
    useParams<{ clusterName: string; subject: string }>();
  const methods = useForm<NewSchemaSubjectRaw>({ mode: 'onChange' });
  const {
    formState: { isDirty, isSubmitting, dirtyFields },
    control,
    handleSubmit,
  } = methods;

  const schema = useAppSelector((state) => selectSchemaById(state, subject));

  const formatedSchema = React.useMemo(() => {
    return schema?.schemaType === SchemaType.PROTOBUF
      ? schema?.schema
      : JSON.stringify(JSON.parse(schema?.schema || '{}'), null, '\t');
  }, [schema]);

  const onSubmit = React.useCallback(async (props: NewSchemaSubjectRaw) => {
    if (!schema) return;

    try {
      if (dirtyFields.newSchema || dirtyFields.schemaType) {
        await schemasApiClient.createNewSchema({
          clusterName,
          newSchemaSubject: {
            ...schema,
            schema: props.newSchema || schema.schema,
            schemaType: props.schemaType || schema.schemaType,
          },
        });
      }

      if (dirtyFields.compatibilityLevel) {
        await schemasApiClient.updateSchemaCompatibilityLevel({
          clusterName,
          subject,
          compatibilityLevel: {
            compatibility: props.compatibilityLevel,
          },
        });
      }

      history.push(clusterSchemaPath(clusterName, subject));
    } catch (e) {
      const err = await getResponse(e as Response);
      dispatch(serverErrorAlertAdded(err));
    }
  }, []);

  if (!schema) return null;

  return (
    <FormProvider {...methods}>
      <PageHeading text="Edit schema" />
      <S.EditWrapper>
        <form onSubmit={handleSubmit(onSubmit)}>
          <div>
            <div>
              <InputLabel>Type</InputLabel>
              <Controller
                control={control}
                rules={{ required: true }}
                name="schemaType"
                render={({ field: { name, onChange } }) => (
                  <Select
                    name={name}
                    value={schema.schemaType}
                    onChange={onChange}
                    minWidth="100%"
                    disabled={isSubmitting}
                    options={Object.keys(SchemaType).map((type) => ({
                      value: type,
                      label: type,
                    }))}
                  />
                )}
              />
            </div>

            <div>
              <InputLabel>Compatibility level</InputLabel>
              <Controller
                control={control}
                name="compatibilityLevel"
                render={({ field: { name, onChange } }) => (
                  <Select
                    name={name}
                    value={schema.compatibilityLevel}
                    onChange={onChange}
                    minWidth="100%"
                    disabled={isSubmitting}
                    options={Object.keys(
                      CompatibilityLevelCompatibilityEnum
                    ).map((level) => ({ value: level, label: level }))}
                  />
                )}
              />
            </div>
          </div>
          <S.EditorsWrapper>
            <div>
              <S.EditorContainer>
                <h4>Latest schema</h4>
                <Editor
                  schemaType={schema?.schemaType}
                  isFixedHeight
                  readOnly
                  height="372px"
                  value={formatedSchema}
                  name="latestSchema"
                  highlightActiveLine={false}
                />
              </S.EditorContainer>
            </div>
            <div>
              <S.EditorContainer>
                <h4>New schema</h4>
                <Controller
                  control={control}
                  name="newSchema"
                  render={({ field: { name, onChange } }) => (
                    <Editor
                      schemaType={schema?.schemaType}
                      readOnly={isSubmitting}
                      defaultValue={formatedSchema}
                      name={name}
                      onChange={onChange}
                    />
                  )}
                />
              </S.EditorContainer>
              <Button
                buttonType="primary"
                buttonSize="M"
                type="submit"
                disabled={!isDirty || isSubmitting}
              >
                Submit
              </Button>
            </div>
          </S.EditorsWrapper>
        </form>
      </S.EditWrapper>
    </FormProvider>
  );
};

export default Edit;